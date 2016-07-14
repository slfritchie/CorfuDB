package org.corfudb.infrastructure;

import com.google.common.collect.BoundType;
import com.google.common.collect.HashMultimap;
import com.google.common.io.Files;
import com.google.common.primitives.Booleans;
import io.netty.channel.ChannelHandlerContext;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.util.JSONUtils;
import org.corfudb.util.Utils;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

import static com.google.common.io.Files.write;

/**
 * The layout server serves layouts, which are used by clients to find the
 * Corfu infrastructure.
 * <p>
 * For replication and high availability, the layout server implements a
 * basic Paxos protocol. The layout server functions as a Paxos acceptor,
 * and accepts proposals from clients consisting of a rank and desired
 * layout. The protocol consists of three rounds:
 * <p>
 * 1)   Prepare(rank) - Clients first contact each server with a rank.
 * If the server responds with ACK, the server promises not to
 * accept any requests with a rank lower than the given rank.
 * If the server responds with LAYOUT_PREPARE_REJECT, the server
 * informs the client of the current high rank and the request is
 * rejected.
 * <p>
 * 2)   Propose(rank,layout) - Clients then contact each server with
 * the previously prepared rank and the desired layout. If no other
 * client has sent a prepare with a higher rank, the layout is
 * persisted, and the server begins serving that layout to other
 * clients. If the server responds with LAYOUT_PROPOSE_REJECT,
 * either another client has sent a prepare with a higher rank,
 * or this was a propose of a previously accepted rank.
 * <p>
 * 3)   Committed(rank) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 * <p>
 * Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
@Slf4j
public class LayoutServer extends AbstractServer {

    /**
     * The options map.
     */
    Map<String, Object> opts;

    /**
     * The current layout.
     */
    Layout currentLayout;

    /**
     * The current phase 1 rank
     */
    Rank phase1Rank;

    /**
     * The current phase 2 rank, which should be equal to the epoch.
     */
    Rank phase2Rank;

    /**
     * The layout proposed in phase 2.
     */
    Layout proposedLayout;

    /**
     * The server router.
     */
    @Getter
    IServerRouter serverRouter;

    /**
     * The layout file, or null if in memory.
     */
    File layoutFile;

    /**
     * Persistent storage for phase1 data in paxos
     */
    File phase1File;

    /**
     * Persistent storage for phase2 data in paxos
     */
    File phase2File;

    /**
     * Configuration manager: disable polling loop
     */
    static public boolean disableConfigMgrPolling = false;

    /**
     * Configuration manager: client runtime
     */
    CorfuRuntime rt = null;

    /**
     * Configuration manager: layout view
     */
    LayoutView lv = null;

    /**
     * Configuration manager: my endpoint name
     */
    String my_endpoint;

    /**
     * Configuration manager: list of layout servers that we monitor for ping'ability.
     */
    String[] history_servers = null;
    NettyClientRouter[] history_routers = null;

    /**
     * Configuration manager: polling history
     */
    int[] history_poll_failures = null;
    int   history_poll_count = 0;
    HashMap<String,Boolean> history_status = null;

    /**
     * Configuration manager: future handle thingie to cancel periodic polling
     */
    ScheduledFuture<?> pollFuture = null;

    /**
     * Configuration manager: all future handle thingies
     */
    @Getter
    public static ConcurrentHashMap<ScheduledFuture<?>,Integer> allPollFutures = new ConcurrentHashMap<>();

    /**
     * A scheduler, which is used to schedule checkpoints and lease renewal
     */
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("Config-Mgr-%d")
                            .build());

    public LayoutServer(Map<String, Object> opts, IServerRouter serverRouter) {
        this.opts = opts;
        this.serverRouter = serverRouter;

        if (opts.get("--log-path") != null) {
            layoutFile = new File(opts.get("--log-path") + File.separator + "layout");
            phase1File = new File(opts.get("--log-path") + File.separator + "phase1Data");
            phase2File = new File(opts.get("--log-path") + File.separator + "phase2Data");
        }

        if ((Boolean) opts.get("--single")) {
            String localAddress = opts.get("--address") + ":" + opts.get("<port>");
            log.info("Single-node mode requested, initializing layout with single log unit and sequencer at {}.",
                    localAddress);
            saveCurrentLayout(new Layout(
                    Collections.singletonList(localAddress),
                    Collections.singletonList(localAddress),
                    Collections.singletonList(new LayoutSegment(
                            Layout.ReplicationMode.CHAIN_REPLICATION,
                            0L,
                            -1L,
                            Collections.singletonList(
                                    new Layout.LayoutStripe(
                                            Collections.singletonList(localAddress)
                                    )
                            )
                    )),
                    0L
            ));

            phase1Rank = phase2Rank = null;
        } else {
            loadCurrentLayout();
            if (currentLayout != null) {
                getServerRouter().setServerEpoch(currentLayout.getEpoch());
            }
            log.info("Layout server started with layout from disk: {}.", currentLayout);
            loadPhase1Data();
            loadPhase2Data();
        }

        // schedule config manager polling.
        if (! disableConfigMgrPolling) {
            my_endpoint = opts.get("--address") + ":" + opts.get("<port>");
            String cmpi = "--cm-poll-interval";
            long poll_interval = (opts.get(cmpi) == null) ? 1 : Utils.parseLong(opts.get(cmpi));
            pollFuture = scheduler.scheduleAtFixedRate(this::configMgrPoll,
                    0, poll_interval, TimeUnit.SECONDS);
            allPollFutures.put(pollFuture, 1);
        }
    }

    protected void finalize() {
        if (pollFuture != null) {
            allPollFutures.remove(pollFuture);
            pollFuture.cancel(true);
        }
    }

    private void configMgrPoll() {
        List<String> layout_servers;

        try {
            if (lv == null) {    // Not bootstrapped yet?
                if (currentLayout == null) {
                    // The local layout server is not bootstrapped, so we have
                    // no hope of participating in Paxos decisions about layout.
                    // We may receive a layout bootstrap sometime in the future,
                    // so do not change the scheduling of this polling task.
                    log.trace("No currentLayout, so skip ConfigMgr poll");
                    return;
                }
                layout_servers = currentLayout.getLayoutServers();
                rt = new CorfuRuntime();
                layout_servers.stream().forEach(ls -> {
                    rt.addLayoutServer(ls);
                });
                // TODO: Warning, rt.connect() will deadlock when run inside a JUnit test.
                // Until I understand this deadlock, we avoid the problem by avoiding any
                // polling at all during JUnit tests (see disableConfigMgrPolling var).
                // SLF reminder: tag tmptag/config-manager-draft1-cf-deadlock, wireExistingRuntimeToTest()?
                rt.connect();
                lv = rt.getLayoutView();  // Can block for arbitrary time
                log.info("Initial client layout for poller = {}", lv.getLayout());

                // TODO: figure out what endpoint *I* am.
                // Workaround: see my_endpoint.
                //
                // So, this is a cool problem.  How the hell do I figure out which
                // endpoint in the layout is *my* server?
                //
                // * So, I know a TCP port number.  That doesn't help if we're
                // deployed on multiple machines and some/all use the same TCP
                // port.
                // * I know the --address key in the 'opts' map.  But that
                // defaults to 'localhost'.  And netty is binding to the "*"
                // address, so other nodes in the cluster can use any IP address
                // they wish on this machine.
                //
                // In the current implementation, I see only one choice:
                // each 'corfu_server' invocation must include an --address=ADDR
                // flag where ADDR is the canonical hostname (or IP address) for
                // for this machine.  That means that the default for --address
                // is only usable in toy localhost-only deployments.  Furthermore,
                // when we get around to having init(8)/init.d(8)/systemd(8)
                // daemon process management, the value of --address must be
                // threaded through those daemon proc managers.
                //
                // HRM, there are uglier hacks available, I suppose.  The client
                // could create a magic cookie and send it in a PING call.  The
                // server side could stash away a history of cookies.  Then we
                // could peek inside the local server, find the cookie history,
                // and see if our cookie is in there.  Bwahahaha, that's icky.
            }
            // Get the current layout using the regular CorfuDB client.
            // The client (in theory) will take care of discovering layout
            // changes that may have taken place while we were stopped/crashed/
            // sleeping/whatever ... AH!  Oops, bad assumption.  The client
            // does *not* perform a discovery/update process.  It appears to
            // accept the layout from the first layout server in the list that
            // is available.  For example, if the list is:
            //   [localhost:8010 @ epoch 0, localhost:8011 @ epoch 1],
            // ... then if the 8010 server is up, the local client does
            // not attempt to fetch 8011's copy and lv.getCurrentLayout()
            // yields epoch 0.
            // TODO: Cobble together a discovery/update function?  Or avoid
            //       the problem by having the Paxos implementation always
            //       fix any non-unanimous servers?
            
            lv = rt.getLayoutView();  // Can block for arbitrary time
            Layout l = lv.getCurrentLayout();
            // log.warn("Hello, world! Client layout = {}", l);
            // For now, assume that lv contains the latest & greatest layout
            layout_servers = l.getLayoutServers();
            if (! layout_servers.contains(my_endpoint)) {
                log.trace("I am not a layout server in epoch " + l.getEpoch() + ", layout server list = " + layout_servers);
                return;
            }
            // If we're here, then it's poll time.
            configMgrPollOnce(l);
        } catch (Exception e) {
            log.warn("TODO Oops, " + e);
            e.printStackTrace();
        }
    }

    // TODO: Yank this into a separate class, refactor, de-C-ify, ...

    void configMgrPollOnce(Layout l) {
        // Are we polling the same servers as last time?  If not, then reset polling state.
        String[] all_servers = l.getAllServers(true).stream().toArray(String[]::new);
        Arrays.sort(all_servers);

        log.warn("TODO FIXME: Poll all servers, not just the layout servers, and when the epoch changes!");
        if (history_servers == null || ! Arrays.equals(history_servers, all_servers)) {
            if (history_status == null) {
                history_status = new HashMap<>();
            }
            log.trace("history_servers change, length = " + all_servers.length);
            history_servers = all_servers;
            history_routers = new NettyClientRouter[all_servers.length];
            history_poll_failures = new int[all_servers.length];
            for (int i = 0; i < all_servers.length; i++) {
                if (! history_status.containsKey(all_servers[i])) {
                    history_status.put(all_servers[i], true);  // Assume it's up until we think it isn't.
                }
                history_routers[i] = new NettyClientRouter(all_servers[i]);
                history_routers[i].setTimeoutConnect(50);
                history_routers[i].setTimeoutRetry(200);
                history_routers[i].setTimeoutResponse(1000);
                history_routers[i].start();
                history_poll_failures[i] = 0;
            }
            history_poll_count = 0;
        } else {
            log.trace("No server list change since last poll.");
        }

        // Poll servers for health.  All ping activity will happen in the background.
        // We probably won't notice changes in this iteration; a future iteration will
        // eventually notice changes to history_poll_failures.
        for (int i = 0; i < history_routers.length; i++) {
            int ii = i;  // Intermediate var just for the sake of having a final for use inside the lambda below
            CompletableFuture.runAsync(() -> {
                // Any changes that we make to history_poll_failures here can possibly
                // race with other async CFs that were launched in earlier/later CFs.
                // We don't care if an increment gets clobbered by another increment:
                //     being off by one isn't a big problem.
                // We don't care if a reset to zero gets clobbered by an increment:
                //     if the endpoint is really pingable, then a later reset to zero
                //     will succeed, probably.
                try {
                    CompletableFuture<Boolean> cf = history_routers[ii].getClient(BaseClient.class).ping();
                    cf.exceptionally(e -> {
                        log.trace(history_servers[ii] + " exception " + e);
                        history_poll_failures[ii]++;
                        return false;
                    });
                    cf.thenAccept((x) -> {
                        if (x == true) {
                            history_poll_failures[ii] = 0;
                        } else {
                            history_poll_failures[ii]++;
                        }
                        return;
                    });

                } catch (Exception e) {
                    log.trace("Ping failed for " + history_servers[ii] + " with " + e);
                    history_poll_failures[ii]++;
                }
            });
        }
        history_poll_count++;

        if (history_poll_count > 3) {
            HashMap<String,Boolean> status_change = new HashMap<>();
            Boolean is_up;

            // Simple failure detector: Is there a change in health?
            for (int i = 0; i < history_servers.length; i++) {
                // TODO: Be a bit smarter than 'more than 2 failures in a row'
                is_up = ! (history_poll_failures[i] > 2);
                if (is_up != history_status.get(history_servers[i])) {
                    log.trace("Change of status: " + history_servers[i] + " " +
                            history_status.get(history_servers[i]) + " -> " + is_up);
                    status_change.put(history_servers[i], is_up);
                }
            }

            // TODO step: If change of health, then change layout.
            if (status_change.size() > 0) {
                log.warn("Status change: " + status_change);

                HashMap<String,Boolean> tmph = new HashMap<String,Boolean>();
                for (String s : history_status.keySet()) {
                    tmph.put(s, history_status.get(s));
                }
                for (String s: status_change.keySet()) {
                    tmph.put(s, status_change.get(s));
                }
                Layout nl = l.newLayout_UpdateDownLists(tmph);
                log.warn("New layout = " + nl);
                // TODO: Replace the layout cluster-wide.

                history_status = tmph;
            } else {
                log.trace("No status change");
            }
        }
    }

    /**
     * Save the current layout to disk, if not in-memory mode.
     */
    public synchronized void saveCurrentLayout(Layout layout) {
        if (layoutFile == null) {
            currentLayout = layout;
            return;
        }
        try {
            write(layout.asJSONString().getBytes(), layoutFile);
            log.info("Layout epoch {} saved to disk.", layout.getEpoch());
            currentLayout = layout;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error saving layout to disk!", e);
        }
    }

    /**
     * Loads the latest committed layout
     * TODO need to figure out the right behaviour when their is error from the persistence layer.
     *
     * @return
     */
    private void loadCurrentLayout() {
        try {
            if (layoutFile == null) {
                log.info("Layout server started, but in-memory mode set without bootstrap. " +
                        "Starting uninitialized layout server.");
                this.currentLayout = null;
            } else if (!layoutFile.exists()) {
                log.warn("Layout server started, but no layout log found. Starting uninitialized layout server.");
                this.currentLayout = null;
            } else {
                String l = Files.toString(layoutFile, Charset.defaultCharset());
                this.currentLayout = Layout.fromJSONString(l);
            }
        } catch (Exception e) {
            log.error("Error reading from layout server", e);
        }
    }

    /**
     * TODO need to figure out what to do when the phase1Rank cannot be saved to disk.
     */
    /**
     * Persists phase1 Rank and also caches it in memory.
     *
     * @param rank
     */
    private synchronized void savePhase1Data(Rank rank) {
        if (phase1File == null) {
            this.phase1Rank = rank;
            return;
        }
        try {
            write(rank.asJSONString().getBytes(), phase1File);
            log.info("Phase1Rank {} saved to disk.", rank);
            this.phase1Rank = rank;
        } catch (Exception e) {
            log.error("Error saving phase1Rank to disk!", e);
        }
    }

    /**
     * Loads the last persisted phase1 data into memory.
     * TODO need to figure out the right behaviour when their is error from the persistence layer.
     *
     * @return
     */
    private void loadPhase1Data() {
        try {
            if (phase1File == null) {
                log.info("No phase1 data persisted so far. ");
            } else if (!phase1File.exists()) {
                log.warn("Phase1 data file found but no phase1 data found!");
            } else {
                String r = Files.toString(phase1File, Charset.defaultCharset());
                phase1Rank = Rank.fromJSONString(r);
            }
        } catch (Exception e) {
            log.error("Error reading phase1 rank from data file for phase1.", e);
        }

    }

    /**
     * Persists  phase2 Data [rank, layout] and caches it in memory
     * TODO need to figure out what to do when the phase1Rank cannot be saved to disk.
     */
    private synchronized void savePhase2Data(Rank rank, Layout layout) {
        if (phase2File == null) {
            this.phase2Rank = rank;
            this.proposedLayout = layout;
            return;
        }
        Phase2Data phase2Data = new Phase2Data(rank, layout);
        try {
            write(phase2Data.asJSONString().getBytes(), phase2File);
            log.info("Phase2Rank {} saved to disk.", phase2Rank);
            this.phase2Rank = rank;
            this.proposedLayout = layout;
        } catch (Exception e) {
            log.error("Error saving phase2Rank to disk!", e);
        }
    }

    /**
     * Returns the last persisted phase2 rank and proposed layout.
     * TODO need to figure out the right behaviour when their is error from the persistence layer.
     *
     * @return
     */
    private void loadPhase2Data() {
        try {
            if (phase2File == null) {
                log.info("No phase2 data witnessed so far. ");
            } else if (!phase2File.exists()) {
                log.warn("Phase2 data file found but no data found!");
            } else {
                String r = Files.toString(phase2File, Charset.defaultCharset());
                Phase2Data phase2Data = Phase2Data.fromJSONString(r);
                phase2Rank = phase2Data.getRank();
                proposedLayout = phase2Data.getLayout();
            }
        } catch (Exception e) {
            log.error("Error reading phase2 rank from data file for phase2.", e);
        }
    }

    //TODO need to figure out if we need to send the complete Rank object in the responses
    //TODO need to figure out how to send back the last accepted value.
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (isShutdown()) return;
        // This server has not been bootstrapped yet, ignore ALL requests except for LAYOUT_BOOTSTRAP
        if (currentLayout == null) {
            if (msg.getMsgType().equals(CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP)) {
                log.info("Bootstrap with new layout={}", ((LayoutMsg) msg).getLayout());

                saveCurrentLayout(((LayoutMsg) msg).getLayout());
                getServerRouter().setServerEpoch(currentLayout.getEpoch());
                //send a response that the bootstrap was successful.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
            } else {
                log.warn("Received message but not bootstrapped! Message={}", msg);
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            }
            return;
        }

        switch (msg.getMsgType()) {
            case LAYOUT_REQUEST:
                r.sendResponse(ctx, msg, new LayoutMsg(currentLayout, CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE));
                break;
            case LAYOUT_BOOTSTRAP:
                // We are already bootstrapped, bootstrap again is not allowed.
                log.warn("Got a request to bootstrap a server which is already bootstrapped, rejecting!");
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
                break;
            case LAYOUT_PREPARE: {
                LayoutRankMsg m = (LayoutRankMsg) msg;
                Rank prepareRank = getRank(m);
                // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
                if (phase1Rank != null && prepareRank.compareTo(phase1Rank) <= 0) {
                    log.debug("Rejected phase 1 prepare of rank={}, phase1Rank={}", prepareRank, phase1Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(proposedLayout, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT));
                } else {
                    savePhase1Data(prepareRank);
                    log.debug("New phase 1 rank={}", phase1Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(proposedLayout, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.ACK));
                }
            }
            break;
            case LAYOUT_PROPOSE: {
                LayoutRankMsg m = (LayoutRankMsg) msg;
                Rank proposeRank = getRank(m);
                Layout proposeLayout = ((LayoutRankMsg) msg).getLayout();
                // This is a propose. If the rank is less than or equal to the phase 1 rank, reject.
                if (phase1Rank != null && proposeRank.compareTo(phase1Rank) != 0) {
                    log.debug("Rejected phase 2 propose of rank={}, phase1Rank={}", proposeRank, phase1Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
                }
                // In addition, if the rank is equal to the current phase 2 rank (already accepted message), reject.
                else if (phase2Rank != null && proposeRank.compareTo(phase2Rank) == 0) {
                    log.debug("Rejected phase 2 propose of rank={}, phase2Rank={}", m.getRank(), phase2Rank);
                    r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase2Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
                } else {
                    log.debug("New phase 2 rank={},  layout={}", proposeRank, proposeLayout);
                    savePhase2Data(proposeRank, proposeLayout);
                    //TODO this should be moved into commit message handling as this is for committed layouts.
                    commitLayout(proposeLayout);
                    r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
                }
            }
            break;
            case LAYOUT_COMMITTED: {
                // Currently we just acknowledge the commit. We could do more than
                // just that.
                r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
            }
            break;
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
        }
    }

    private synchronized void commitLayout(Layout layout) {
        saveCurrentLayout(layout);
        serverRouter.setServerEpoch(currentLayout.getEpoch());
        // this is needed so that we do not keep
        // choosing the same value over each slot.
        //TODO move this into commit message processing and then uncomment
        //clearPhase2Data();
    }

    private void clearPhase2Data() {
        phase2Rank = null;
        proposedLayout = null;
        if (phase2File != null) {
            try {
                Files.write(new byte[0], phase2File);
            } catch (IOException e) {
                log.error("Error clearing phase2 Data from disk!", e);
            }
        }
    }

    private Rank getRank(LayoutRankMsg msg) {
        return new Rank(msg.getRank(), msg.getClientID());
    }

    @Override
    public void reset() {

    }

    /**
     * Phase2 data consists of rank and the proposed layout.
     * The container class provides a convenience to persist and retrieve
     * these two pieces of data together.
     */
    @Data
    @AllArgsConstructor
    static class Phase2Data {
        Rank rank;
        Layout layout;

        /**
         * Get a layout from a JSON string.
         */
        public static Phase2Data fromJSONString(String json) {
            return JSONUtils.parser.fromJson(json, Phase2Data.class);
        }

        /**
         * Get the layout as a JSON string.
         */
        public String asJSONString() {
            return JSONUtils.parser.toJson(this);
        }
    }
}
