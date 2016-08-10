package org.corfudb.infrastructure;

import com.ericsson.otp.erlang.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.channel.ChannelHandlerContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.cmdlets.CmdletRouter;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.LayoutMsg;
import org.corfudb.protocols.wireprotocol.LayoutRankMsg;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.Layout.LayoutSegment;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.util.Utils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

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
 * 3)   Committed(rank, layout) - Clients then send a hint to each layout
 * server that a new rank has been accepted by a quorum of
 * servers.
 * <p>
 * Created by mwei on 12/8/15.
 */
//TODO Finer grained synchronization needed for this class.
//TODO Need a janitor to cleanup old phases data and to fill up holes in layout history.
@Slf4j
public class LayoutServer extends AbstractServer {

    public static final String PREFIX_LAYOUT = "LAYOUT";
    public static final String KEY_LAYOUT = "CURRENT";
    public static final String PREFIX_PHASE_1 = "PHASE_1";
    public static final String KEY_SUFFIX_PHASE_1 = "RANK";
    public static final String PREFIX_PHASE_2 = "PHASE_2";
    public static final String KEY_SUFFIX_PHASE_2 = "DATA";
    public static final String PREFIX_LAYOUTS = "LAYOUTS";

    /**
     * The options map.
     */
    Map<String, Object> opts;

    /**
     * The server router.
     */
    @Getter
    IServerRouter serverRouter;

    private DataStore dataStore;

    /**
     * Configuration manager: disable polling loop
     */
    public static boolean disableConfigMgrPolling = false; // QQQ debugging only, put me back to false!!!!

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
    public static ScheduledFuture<?> pollFuture = null;
    private static Object pollFutureLock = new Object();

    /**
     * TODO DELETE ME.
     */
    Layout todo_layout_source_kludge = null;

    /**
     * TODO refactor/move to another class
     */
    private Thread erlNodeThread;

    /**
     * TODO refactor/move to another class
     */
    private static OtpNode otpNode = null;
    private static Object otpNodeLock = new Object();

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

        reboot();

        // schedule config manager polling.
        if (! disableConfigMgrPolling) {
            start_config_manager_polling();
        }

        // QuickCheck: Create the distributed Erlang message handling threads
        Object test_mode = opts.get("--quickcheck-test-mode");
        if (test_mode != null && (Boolean) test_mode) {
            start_quickcheck_test_mode();
        }
    }

    //TODO need to figure out if we need to send the complete Rank object in the responses
    @Override
    public void handleMessage(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (isShutdown()) return;
        // This server has not been bootstrapped yet, ignore ALL requests except for LAYOUT_BOOTSTRAP
        if (getCurrentLayout() == null && !msg.getMsgType().equals(CorfuMsg.CorfuMsgType.LAYOUT_BOOTSTRAP)) {
            log.warn("Received message but not bootstrapped! Message={}", msg);
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP));
            return;
        }
        switch (msg.getMsgType()) {
            case LAYOUT_REQUEST:
                r.sendResponse(ctx, msg, new LayoutMsg(getCurrentLayout(), CorfuMsg.CorfuMsgType.LAYOUT_RESPONSE));
                break;
            case LAYOUT_BOOTSTRAP:
                handleMessageLayoutBootStrap(msg, ctx, r);
                break;
            case LAYOUT_PREPARE:
                handleMessageLayoutPrepare((LayoutRankMsg) msg, ctx, r);
                break;
            case LAYOUT_PROPOSE:
                handleMessageLayoutPropose((LayoutRankMsg) msg, ctx, r);
                break;
            case LAYOUT_COMMITTED: {
                handleMessageLayoutCommit((LayoutRankMsg) msg, ctx, r);
            }
            break;
            default:
                log.warn("Unknown message type {} passed to handler!", msg.getMsgType());
                throw new RuntimeException("Unsupported message passed to handler!");
        }
    }

    /**
     * Reset the server, deleting persistent state on disk prior to rebooting.
     */

    @Override
    public void reset() {
        String d = dataStore.getLogDir();
        if (d == null) {
            System.out.println("WTF WTFWTFWTF");
        }
        if (d != null) {
            Path dir = FileSystems.getDefault().getPath(d);
            String prefixes[] = new String[] {PREFIX_LAYOUT, KEY_LAYOUT, PREFIX_PHASE_1, PREFIX_PHASE_2,
                    PREFIX_LAYOUTS, "SERVER_EPOCH"};

            for (String pfx : prefixes) {
                try (DirectoryStream<Path> stream =
                             Files.newDirectoryStream(dir, pfx + "_*")) {
                    for (Path entry : stream) {
                        System.out.println("Deleting " + entry);
                        Files.delete(entry);
                    }
                } catch (IOException e) {
                    log.error("reset: error deleting prefix " + pfx + ": " + e.toString());
                }
            }
            try (DirectoryStream<Path> stream =
                         Files.newDirectoryStream(dir, "*")) {
                for (Path entry : stream) {
                    System.out.println("Remaining file " + entry);
                }
            } catch (IOException e) {
                log.error("reset: error deleting prefix: " + e.toString());
            }
        }
        reboot();
    }

    /**
     * Reboot the server, using persistent state on disk to restart.
     */
    @Override
    public void reboot() {
        this.dataStore = new DataStore(opts);
        if ((Boolean) opts.get("--single")) {
            String localAddress = opts.get("--address") + ":" + opts.get("<port>");
            log.info("Single-node mode requested, initializing layout with single log unit and sequencer at {}.",
                    localAddress);
            setCurrentLayout(new Layout(
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
        } else {
            Layout currentLayout = getCurrentLayout();
            if (currentLayout != null) {
                getServerRouter().setServerEpoch(currentLayout.getEpoch());
            }
            log.info("Layout server started with layout from disk: {}.", currentLayout);
        }
    }

    // Helper Methods

    /**
     * Sets the new layout if the server has not been bootstrapped with one already.
     *
     * @param msg
     * @param ctx
     * @param r
     */
    public synchronized void handleMessageLayoutBootStrap(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (getCurrentLayout() == null) {
            log.info("Bootstrap with new layout={}", ((LayoutMsg) msg).getLayout());
            setCurrentLayout(((LayoutMsg) msg).getLayout());
            getServerRouter().setServerEpoch(getCurrentLayout().getEpoch());
            //send a response that the bootstrap was successful.
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
        } else {
            // We are already bootstrapped, bootstrap again is not allowed.
            log.warn("Got a request to bootstrap a server which is already bootstrapped, rejecting!");
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_ALREADY_BOOTSTRAP));
        }
    }

    /**
     * Accepts a prepare message if the rank is higher than any accepted so far.
     * @param msg
     * @param ctx
     * @param r
     */
    // TODO this can work under a separate lock for this step as it does not change the global components
    public synchronized void handleMessageLayoutPrepare(LayoutRankMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        Rank prepareRank = getRank(msg);
        Rank phase1Rank = getPhase1Rank();
        Layout proposedLayout = getProposedLayout();
        // This is a prepare. If the rank is less than or equal to the phase 1 rank, reject.
        if (phase1Rank != null && prepareRank.compareTo(phase1Rank) <= 0) {
            log.debug("Rejected phase 1 prepare of rank={}, phase1Rank={}", prepareRank, phase1Rank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(proposedLayout, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_REJECT));
        } else {
            setPhase1Rank(prepareRank);
            log.debug("New phase 1 rank={}", getPhase1Rank());
            r.sendResponse(ctx, msg, new LayoutRankMsg(proposedLayout, prepareRank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PREPARE_ACK));
        }
    }

    /**
     * Accepts a proposal for which it had accepted in the prepare phase.
     * A minor optimization is to reject any duplicate propose messages.
     * @param msg
     * @param ctx
     * @param r
     */
    public synchronized void handleMessageLayoutPropose(LayoutRankMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        Rank proposeRank = getRank(msg);
        Layout proposeLayout = msg.getLayout();
        Rank phase1Rank = getPhase1Rank();
        Rank phase2Rank = getPhase2Rank();
        // This is a propose. If no prepare, reject.
        if (phase1Rank == null) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank=none", proposeRank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(null, -1, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
            return;
        }
        // This is a propose. If the rank is less than or equal to the phase 1 rank, reject.
        if (proposeRank.compareTo(phase1Rank) != 0) {
            log.debug("Rejected phase 2 propose of rank={}, phase1Rank={}", proposeRank, phase1Rank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase1Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
            return;
        }
        // In addition, if the rank is equal to the current phase 2 rank (already accepted message), reject.
        // This can happen in case of duplicate messages.
        if (phase2Rank != null && proposeRank.compareTo(phase2Rank) == 0) {
            log.debug("Rejected phase 2 propose of rank={}, phase2Rank={}", proposeRank, phase2Rank);
            r.sendResponse(ctx, msg, new LayoutRankMsg(null, phase2Rank.getRank(), CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
            return;
        }

        log.debug("New phase 2 rank={},  layout={}", proposeRank, proposeLayout);
        setPhase2Data(new Phase2Data(proposeRank, proposeLayout));
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
    }

    /**
     * Accepts any committed layouts for the current epoch or newer epochs.
     * As part of the accept, the server changes it's current layout and epoch.
     * @param msg
     * @param ctx
     * @param r
     */
    // TODO If a server does not get SET_EPOCH layout commit message cannot reach it
    // TODO as this message is not set to ignore EPOCH.
    // TODO How do we handle holes in history if let in layout commit message. Maybe we have a hole filling process
    // TODO how do reject the older epoch commits, should it be an explicit NACK.
    public synchronized void handleMessageLayoutCommit(LayoutRankMsg msg, ChannelHandlerContext ctx, IServerRouter r) {
        if (msg.getLayout().getEpoch() < 1 || msg.getLayout().getEpoch() < serverRouter.getServerEpoch()) {
            System.out.printf("Rejected commit epoch=%d, my epoch=%d\n", msg.getEpoch(), serverRouter.getServerEpoch());
            // doesn't work: r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.NACK));
            r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.LAYOUT_NOBOOTSTRAP)); // TODO: new response code?
            return;
        }
/*
        // SLF TODO: Make the Layout class implement Comparable?
        if (getPhase2Data() == null || ! msg.getLayout().toString().equals(getPhase2Data().getLayout().toString())) {
            log.debug("Rejected committed rank={}, phase2Rank={}", msg.getRank(), getPhase2Rank());
            System.out.printf("Rejected committed rank=%d\n", msg.getRank());
            System.out.printf("Rejected committed rank=%d, phase2Rank=%s\n", msg.getRank(), getPhase2Rank() == null ? "nuLL" : getPhase2Rank().toString());
            System.out.printf("Rejected committed msg %s\n", msg.getLayout().toString());
            System.out.printf("Rejected committed myy %s\n", getPhase2Data() == null ? "nuLl" : getPhase2Data().getLayout().toString());
            r.sendResponse(ctx, msg, new LayoutRankMsg(null, -77, CorfuMsg.CorfuMsgType.LAYOUT_PROPOSE_REJECT));
            return;
        }
*/
        Layout commitLayout = msg.getLayout();
        setCurrentLayout(commitLayout);
        serverRouter.setServerEpoch(commitLayout.getEpoch());
        r.sendResponse(ctx, msg, new CorfuMsg(CorfuMsg.CorfuMsgType.ACK));
    }

    public Layout getCurrentLayout() {
        return dataStore.get(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT);
    }

    public void setCurrentLayout(Layout layout) {
        dataStore.put(Layout.class, PREFIX_LAYOUT, KEY_LAYOUT, layout);
        // set the layout in history as well
        setLayoutInHistory(layout);
    }

    public Rank getPhase1Rank() {
        return dataStore.get(Rank.class, PREFIX_PHASE_1, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_1);
    }

    public void setPhase1Rank(Rank rank) {
        dataStore.put(Rank.class, PREFIX_PHASE_1, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_1, rank);
    }

    public Phase2Data getPhase2Data() {
        return dataStore.get(Phase2Data.class, PREFIX_PHASE_2, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_2);
    }

    public void setPhase2Data(Phase2Data phase2Data) {
        dataStore.put(Phase2Data.class, PREFIX_PHASE_2, serverRouter.getServerEpoch() + KEY_SUFFIX_PHASE_2, phase2Data);
    }

    public void setLayoutInHistory(Layout layout) {
        dataStore.put(Layout.class, PREFIX_LAYOUTS, String.valueOf(layout.getEpoch()), layout);
    }

    public List<Layout> getLayoutHistory() {
        List<Layout> layouts = dataStore.getAll(Layout.class, PREFIX_LAYOUTS);
        Collections.sort(layouts, (a, b) -> {
            if (a.getEpoch() > b.getEpoch()) {
                return 1;
            } else if (a.getEpoch() < b.getEpoch()) {
                return -1;
            } else {
                return 0;
            }
        });
        return layouts;
    }

    public Rank getPhase2Rank() {
        Phase2Data phase2Data = getPhase2Data();
        if (phase2Data != null) {
            return phase2Data.getRank();
        }
        return null;
    }

    public Layout getProposedLayout() {
        Phase2Data phase2Data = getPhase2Data();
        if (phase2Data != null) {
            return phase2Data.getLayout();
        }
        return null;
    }

    private Rank getRank(LayoutRankMsg msg) {
        return new Rank(msg.getRank(), msg.getClientID());
    }

    protected void finalize() {
        //
    }

    private void start_config_manager_polling() {
        synchronized (pollFutureLock) {
            if (pollFuture == null) {
                my_endpoint = opts.get("--address") + ":" + opts.get("<port>");
                String cmpi = "--cm-poll-interval";
                long poll_interval = (opts.get(cmpi) == null) ? 1 : Utils.parseLong(opts.get(cmpi));
                pollFuture = scheduler.scheduleAtFixedRate(this::configMgrPoll,
                        0, poll_interval, TimeUnit.SECONDS);
            }
        }
    }

    private void configMgrPoll() {
        File f;
        List<String> layout_servers;

        System.out.printf("Poll top, "); System.out.flush();
        f = new File("/tmp/shutdown-layout-server");
        if (f.canRead()) {
            System.out.println("SHUTDOWN FOUND");
            shutdown();
        }
        f = new File("/tmp/abort-poll");
        if (f.canRead()) {
            System.out.println("disableConfigMgrPolling = true");
            disableConfigMgrPolling = true;
        } else {
            disableConfigMgrPolling = false;
        }

        // NOTE: This polling action is associated with a particular LayoutServer
        //       object.  If that object is shutdown, then polling will stop,
        //       no matter how many other LayoutServer objects have been created
        //       and are not shut down.
        if (isShutdown() || disableConfigMgrPolling) {
            log.warn("I am shutdown, skipping configMgrPoll");
            return;
        }
        try {
            if (lv == null) {    // Not bootstrapped yet?
                Layout currentLayout = getCurrentLayout();
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
            Layout l;

            if (todo_layout_source_kludge == null) {
                l = lv.getCurrentLayout();
            } else {
                l = todo_layout_source_kludge;
            }
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
        String[] all_servers = l.getAllServers().stream().toArray(String[]::new);
        Arrays.sort(all_servers);

        // log.warn("TODO FIXME: Poll all servers, not just the layout servers, and when the epoch changes!");
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
                Layout nl = l; // l.newLayout_UpdateDownLists(tmph);
                log.warn("New layout = " + nl);
                // TODO: Replace the layout cluster-wide.
                todo_layout_source_kludge = nl;

                history_status = tmph;
            } else {
                log.trace("No status change");
            }
        }
    }

    private void start_quickcheck_test_mode() {
        synchronized (otpNodeLock) {
            if (otpNode == null) {
                int port = Integer.parseInt((String) opts.get("<port>"));
                String nodename = "corfu-" + port;
                try {
                    otpNode = new OtpNode(nodename);

                    System.out.println("\n\n***************** Creating lots of OtpNode Threads ************\n\n");
                    Thread erlNodeThread0 = new Thread(this::runErlMbox0);
                    erlNodeThread0.start();
                    Thread erlNodeThread1 = new Thread(this::runErlMbox1);
                    erlNodeThread1.start();
                    Thread erlNodeThread2 = new Thread(this::runErlMbox2);
                    erlNodeThread2.start();
                    Thread erlNodeThread3 = new Thread(this::runErlMbox3);
                    erlNodeThread3.start();
                    Thread erlNodeThread4 = new Thread(this::runErlMbox4);
                    erlNodeThread4.start();
                    Thread erlNodeThread5 = new Thread(this::runErlMbox5);
                    erlNodeThread5.start();
                    Thread erlNodeThread6 = new Thread(this::runErlMbox6);
                    erlNodeThread6.start();
                    Thread erlNodeThread7 = new Thread(this::runErlMbox7);
                    erlNodeThread7.start();
                    Thread erlNodeThread8 = new Thread(this::runErlMbox8);
                    erlNodeThread8.start();
                    Thread erlNodeThread9 = new Thread(this::runErlMbox9);
                    erlNodeThread9.start();
                    Thread erlNodeThread10 = new Thread(this::runErlMbox10);
                    erlNodeThread10.start();
                    Thread erlNodeThread11 = new Thread(this::runErlMbox11);
                    erlNodeThread11.start();
                    Thread erlNodeThread12 = new Thread(this::runErlMbox12);
                    erlNodeThread12.start();
                    Thread erlNodeThread13 = new Thread(this::runErlMbox13);
                    erlNodeThread13.start();
                    Thread erlNodeThread14 = new Thread(this::runErlMbox14);
                    erlNodeThread14.start();
                    Thread erlNodeThread15 = new Thread(this::runErlMbox15);
                    erlNodeThread15.start();
                } catch (IOException e) {
                    log.info("Error creating OtpNode {}: {}", nodename, e);
                }
            }
        }
    }

    public void runErlMbox0() { runErlMbox(0); }
    public void runErlMbox1() { runErlMbox(1); }
    public void runErlMbox2() { runErlMbox(2); }
    public void runErlMbox3() { runErlMbox(3); }
    public void runErlMbox4() { runErlMbox(4); }
    public void runErlMbox5() { runErlMbox(5); }
    public void runErlMbox6() { runErlMbox(6); }
    public void runErlMbox7() { runErlMbox(7); }
    public void runErlMbox8() { runErlMbox(8); }
    public void runErlMbox9() { runErlMbox(9); }
    public void runErlMbox10() { runErlMbox(10); }
    public void runErlMbox11() { runErlMbox(11); }
    public void runErlMbox12() { runErlMbox(12); }
    public void runErlMbox13() { runErlMbox(13); }
    public void runErlMbox14() { runErlMbox(14); }
    public void runErlMbox15() { runErlMbox(15); }

    public void runErlMbox(int num) {
        Thread.currentThread().setName("DistErl-" + num);
        try {
            OtpMbox mbox = otpNode.createMbox("cmdlet" + num);

            OtpErlangObject o;
            OtpErlangTuple msg;
            OtpErlangPid from;
            CmdletRouter cr = new CmdletRouter();

            while (true) {
                try {
                    o = mbox.receive();
                    // System.err.print("{"); System.err.flush(); // Thread.sleep(100);
                    if (o instanceof OtpErlangTuple) {
                        msg = (OtpErlangTuple) o;
                        from = (OtpErlangPid) msg.elementAt(0);
                        OtpErlangObject id = msg.elementAt(1);
                        OtpErlangList cmd = (OtpErlangList) msg.elementAt(2);
                        String[] sopts = new String[cmd.elements().length];
                        for (int i = 0; i < sopts.length; i++) {
                            if (cmd.elementAt(i).getClass() == OtpErlangList.class) {
                                // We're expecting a string always, but
                                // the Erlang side will send an empty list
                                // for a zero length string.
                                sopts[i] = "";
                            } else {
                                sopts[i] = ((OtpErlangString) cmd.elementAt(i))
                                    .stringValue();
                            }
                        }
                        String[] res = cr.main2(sopts);
                        OtpErlangObject[] reslist = new OtpErlangObject[res.length];
                        for (int i = 0; i < res.length; i++) {
                            reslist[i] = new OtpErlangString(res[i]);
                        }
                        OtpErlangList reply_reslist = new OtpErlangList(reslist);
                        OtpErlangTuple reply = new OtpErlangTuple(new OtpErlangObject[] { id, reply_reslist });
                        mbox.send(from, reply);
                        // System.err.print("}"); System.err.flush();
                    }
                } catch (Exception e) {
                    System.out.println("Qxx " + e);
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            log.warn("Yo, bummer: " + e);
        }
    }
}
