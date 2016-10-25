package org.corfudb.cmdlets;

import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.codehaus.plexus.util.ExceptionUtils;
import org.corfudb.infrastructure.CorfuServer;
import org.corfudb.infrastructure.LayoutServer;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.LayoutClient;
import org.corfudb.runtime.clients.LayoutPrepareResponse;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;
import org.corfudb.runtime.view.LayoutView;
import org.corfudb.util.GitRepositoryState;
import org.corfudb.util.Utils;
import org.docopt.Docopt;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import static org.fusesource.jansi.Ansi.Color.*;
import static org.fusesource.jansi.Ansi.ansi;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class corfu_layout implements ICmdlet {

    private static Map<String, NettyClientRouter> routers = new ConcurrentHashMap<>();
    private static Map<String, LayoutView> layoutViews = new ConcurrentHashMap<>();
    private static Map<String, CorfuRuntime> setEpochRTs = new ConcurrentHashMap<>();

    private static final String USAGE =
            "corfu_layout, directly interact with a layout server.\n"
                    + "\n"
                    + "Usage:\n"
                    + "\tcorfu_layout query <address>:<port> [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout bootstrap <address>:<port> [-l <layout>|-s] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout set_epoch <address>:<port> -e epoch [-d <level>] [-p <qapp>]\n"
                    + "\tcorfu_layout prepare <address>:<port> -r <rank> [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout propose <address>:<port> -r <rank> [-l <layout>|-s] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout committed <address>:<port> -r <rank> [-l <layout>] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\tcorfu_layout update_layout <address>:<port> -r <rank> [-l <layout>] [-d <level>] [-e epoch] [-p <qapp>]\n"
                    + "\n"
                    + "Options:\n"
                    + " -l <layout>, --layout-file=<layout>  Path to a JSON file describing the \n"
                    + "                                      desired layout. If not specified and\n"
                    + "                                      --single not specified, takes input from stdin.\n"
                    + " -s, --single                         Generate a single node layout, with the node \n"
                    + "                                      itself serving all roles.                  \n"
                    + " -r <rank>, --rank=<rank>             The rank to use for a Paxos operation. \n"
                    + " -d <level>, --log-level=<level>      Set the logging level, valid levels are: \n"
                    + "                                      ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].\n"
                    + " -e <epoch>, --epoch=<epoch>          Set the epoch for the client request PDU."
                    + " -p <qapp>, --quickcheck-ap-prefix=<qapp> Set QuickCheck addressportPrefix."
                    + " -h, --help  Show this screen\n"
                    + " --version  Show version\n";

    @Override
    public String[] main(String[] args) {
        if (args != null && args.length > 0 && args[0].contentEquals("reset")) {
            log.trace("corfu_layout top: reset");

            // The client views get really confused when the epoch goes backward.
            // (N.B. layoutViews is only used for "update_layout" testing via QuickCheck.)
            // Discard the current layoutViews and get some new ones....
            //
            // Unfortunately, the runtime's stop() method is broken: it doesn't close all TCP
            // connections, and leaks Netty client-side worker threads.
            // FIX ME.  These leaks make QuickCheck testing extremely difficult.
            layoutViews.forEach((k, v_layout) -> {
                /////////////////////////////////// v_layout.getRuntime().stop(true);
                });
            layoutViews = new ConcurrentHashMap<>();

            LayoutServer ls = CorfuServer.getLayoutServer();
            if (ls != null) {
                ls.reset();
                return cmdlet.ok();
            } else {
                return cmdlet.err("No active layout server");
            }
        }
        if (args != null && args.length > 0 && args[0].contentEquals("reboot")) {
            LayoutServer ls = CorfuServer.getLayoutServer();
            if (ls != null) {
                ls.reboot();
                return cmdlet.ok();
            } else {
                return cmdlet.err("No active layout server");
            }
        }

        // Parse the options given, using docopt.
        Map<String, Object> opts =
                new Docopt(USAGE).withVersion(GitRepositoryState.getRepositoryState().describe).parse(args);

        // Configure base options
        configureBase(opts);

        // Parse host address and port
        String addressport = (String) opts.get("<address>:<port>");
        String host = addressport.split(":")[0];
        Integer port = Integer.parseInt(addressport.split(":")[1]);
        String qapp = (String) opts.get("<qapp>");
        String addressportPrefix = "";
        if (qapp != null) {
            addressportPrefix = qapp;
        }

        NettyClientRouter router;
        if ((router = routers.get(addressportPrefix + addressport)) == null) {
            // Create a client router and get layout.
            log.trace("Creating router for {} ++ {}:{}", addressportPrefix, port);
            router = new NettyClientRouter(host, port);
            router.addClient(new BaseClient())
                .addClient(new LayoutClient())
                .start();
            routers.putIfAbsent(addressportPrefix + addressport, router);
        }
        router = routers.get(addressportPrefix + addressport);

        Long clientEpoch = Long.parseLong((String) opts.get("--epoch"));
        if (opts.get("--epoch") != null) {
            log.trace("Specify router's epoch as " + clientEpoch);
            router.setEpoch(clientEpoch);
        } else {
            try {
                Layout l = router.getClient(LayoutClient.class).getLayout().get();
                if (l != null) {
                    log.trace("Set router's epoch to " + l.getEpoch());
                    router.setEpoch(l.getEpoch());
                } else {
                    log.trace("Cannot set router's epoch");
                }
            } catch (Exception e) {
                return cmdlet.err("ERROR Exception getting initial epoch " + e.getCause());
            }
        }

        if ((Boolean) opts.get("query")) {
            try {
                Layout l = router.getClient(LayoutClient.class).getLayout().get();
                Gson gs = new GsonBuilder().setPrettyPrinting().create();
                return cmdlet.ok("layout: " + gs.toJson(l));
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == WrongEpochException.class) {
                    WrongEpochException we = (WrongEpochException) ex.getCause();
                    return cmdlet.err("Exception during query",
                            ex.getCause().toString(),
                            "correctEpoch: " + we.getCorrectEpoch(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during query",
                            ex.getCause().toString(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("ERROR Exception getting layout" + e);
            }
        } else if ((Boolean) opts.get("bootstrap")) {
            Layout l = getLayout(opts);
            log.debug("Bootstrapping with layout {}", l);
            try {
                if (router.getClient(LayoutClient.class).bootstrapLayout(l).get()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("NACK");
                }
            } catch (ExecutionException ex) {
                return cmdlet.err("Exception bootstrapping layout", ex.getCause().toString());
            } catch (Exception e) {
                return cmdlet.err("Exception bootstrapping layout", e.toString());
            }
        } else if ((Boolean) opts.get("set_epoch")) {
            long epoch = Long.parseLong((String) opts.get("--epoch"));
            log.debug("Set epoch with new epoch={}", epoch);
            try {
                CorfuRuntime rt;
                if ((rt = setEpochRTs.get(addressport)) == null) {
                    log.trace("Creating CorfuRuntime for set_epoch for {} ", addressport);
                    rt = new CorfuRuntime().addLayoutServer(addressport);
                    setEpochRTs.putIfAbsent(addressport, rt);
                }
                rt = setEpochRTs.get(addressport);

                List<String> ls = new ArrayList(1);
                ls.add(addressport);
                List<String> none1 = new ArrayList(0);
                List<Layout.LayoutSegment> none2 = new ArrayList(0);
                Layout fooLayout = new Layout(ls, none1, none2, epoch);
                fooLayout.setRuntime(rt);
                fooLayout.moveServersToEpoch();
                return cmdlet.ok();
            } catch (WrongEpochException we) {
                return cmdlet.err("Exception during set_epoch",
                        we.getCause() == null ? "WrongEpochException" : we.getCause().toString(),
                        "correctEpoch: " + we.getCorrectEpoch(),
                        "stack: " + ExceptionUtils.getStackTrace(we));
            } catch (Exception e) {
                return cmdlet.err("Exception during set_epoch", e.toString(), ExceptionUtils.getStackTrace(e));
            }
        } else if ((Boolean) opts.get("prepare")) {
            long rank = Long.parseLong((String) opts.get("--rank"));
            log.debug("Prepare with new rank={}", rank);
            try {
                LayoutPrepareResponse r = router.getClient(LayoutClient.class).prepare(rank).get();
                Layout r_layout = r.getLayout();
                if (r.isAccepted()) {
                    if (r_layout == null) {
                        return cmdlet.ok("ignored: ignored");
                    } else {
                        return cmdlet.ok("layout: " + r_layout.asJSONString());
                    }
                } else {
                    if (r_layout == null) {
                        return cmdlet.err("ACK");
                    } else {
                        return cmdlet.err("ACK", "layout: " + r_layout.asJSONString());
                    }
                }
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == OutrankedException.class) {
                    OutrankedException oe = (OutrankedException) ex.getCause();
                    return cmdlet.err("Exception during prepare",
                                ex.getCause().toString(),
                                "newRank: " + Long.toString(oe.getNewRank()),
                                "layout: " + (oe.getLayout() == null ? "" : oe.getLayout().asJSONString()));
                } else if (ex.getCause().getClass() == WrongEpochException.class) {
                    WrongEpochException we = (WrongEpochException) ex.getCause();
                    return cmdlet.err("Exception during prepare",
                            ex.getCause().toString(),
                            "correctEpoch: " + we.getCorrectEpoch(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during prepare",
                                ex.getCause().toString(),
                                "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("Exception during prepare", e.toString(), ExceptionUtils.getStackTrace(e));
            }
        } else if ((Boolean) opts.get("propose")) {
            long rank = Long.parseLong((String) opts.get("--rank"));
            Layout l = getLayout(opts);
            log.debug("Propose with new rank={}, layout={}", rank, l);
            try {
                if (router.getClient(LayoutClient.class).propose(rank, l).get()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("NACK");
                }
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == OutrankedException.class) {
                    OutrankedException oe = (OutrankedException) ex.getCause();
                    return cmdlet.err("Exception during propose",
                            ex.getCause().toString(),
                            "newRank: " + Long.toString(oe.getNewRank()),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else if (ex.getCause().getClass() == WrongEpochException.class) {
                        WrongEpochException we = (WrongEpochException) ex.getCause();
                        return cmdlet.err("Exception during propose",
                                ex.getCause().toString(),
                                "correctEpoch: " + we.getCorrectEpoch(),
                                "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during propose",
                                ex.getCause().toString(),
                                "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("Exception during propose",
                            e.toString(),
                            "stack: " + ExceptionUtils.getStackTrace(e));
            }
        } else if ((Boolean) opts.get("committed")) {
            long rank = Long.parseLong((String) opts.get("--rank"));
            Layout l = getLayout(opts);
            log.debug("Propose with new rank={}", rank);
            try {
                if (router.getClient(LayoutClient.class).committed(rank, l).get()) {
                    return cmdlet.ok();
                } else {
                    return cmdlet.err("NACK");
                }
            } catch (ExecutionException ex) {
                if (ex.getCause().getClass() == WrongEpochException.class) {
                    WrongEpochException we = (WrongEpochException) ex.getCause();
                    return cmdlet.err("Exception during commit",
                            ex.getCause().toString(),
                            "correctEpoch: " + we.getCorrectEpoch(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                } else {
                    return cmdlet.err("Exception during commit",
                            ex.getCause().toString(),
                            "stack: " + ExceptionUtils.getStackTrace(ex));
                }
            } catch (Exception e) {
                return cmdlet.err("Exception during commit",
                        e.toString(),
                        "stack: " + ExceptionUtils.getStackTrace(e));
            }
        } else if ((Boolean) opts.get("update_layout")) {
            // This is not strictly a low-level primitive for the layout Paxos protocol.
            // Rather, it's here at glue for QuickCheck testing of higher-level
            // layout API exercise in the same spirit that layout_qc.erl tests
            // the lower-level Paxos protocol.

            LayoutView lv;
            CorfuRuntime rt = configureRuntimeAddrPort(opts);
            if ((lv = layoutViews.get(addressportPrefix + addressport)) == null) {
                log.trace("Creating LayoutView for {} ++ {}:{}", addressportPrefix, port);
                lv = new LayoutView(rt);
                layoutViews.putIfAbsent(addressportPrefix + addressport, lv);
            }
            lv = layoutViews.get(addressportPrefix + addressport);

            Layout l = getLayout(opts);
            l.setRuntime(rt);
            long rank = Long.parseLong((String) opts.get("--rank"));
            log.debug("update_layout with layout={}, rank={}, ", l, rank);
            try {
                // Important: we must add a CorfuRuntime to the Layout object l.
                // If we don't, then we'll get a NullPointerException deep in the
                // guts of LayoutView.committed() which tries to use the layout's
                // runtime member.
                log.trace("Specify (2) router's epoch as " + clientEpoch);
                rt.getRouter((String) opts.get("<address>:<port>")).setEpoch(clientEpoch);
                l.setRuntime(rt);
                lv.updateLayout(l, rank);
                return cmdlet.ok();
            } catch (WrongEpochException we) {
                return cmdlet.err("Exception (1) during updateLayout",
                        we.getCause() == null ? "WrongEpochException" : we.getCause().toString(),
                        "correctEpoch: " + we.getCorrectEpoch(),
                        "stack: " + ExceptionUtils.getStackTrace(we));
            } catch (OutrankedException oe) {
                return cmdlet.err("Exception (2) during updateLayout",
                    oe.getCause() == null ? "OutrankedException" : oe.getCause().toString(),
                    "newRank: " + Long.toString(oe.getNewRank()),
                    "stack: " + ExceptionUtils.getStackTrace(oe));
            } catch (QuorumUnreachableException ue) {
                return cmdlet.err("Exception (2) during updateLayout",
                        ue.getCause().toString());
            }
        }
        return cmdlet.err("Hush, compiler.");
    }

    Layout getLayout(Map<String, Object> opts) {
        Layout oLayout = null;

        if ((Boolean) opts.getOrDefault("--single", false)) {
            String localAddress = (String) opts.get("<address>:<port>");
            log.info("Single-node mode requested, initializing layout with single log unit and sequencer at {}.",
                    localAddress);
            oLayout = new Layout(
                    Collections.singletonList(localAddress),
                    Collections.singletonList(localAddress),
                    Collections.singletonList(new Layout.LayoutSegment(
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
            );
        } else if (opts.get("--layout-file") != null) {
            try {
                String f = (String) opts.get("--layout-file");
                String layoutJson = new String(Files.readAllBytes(Paths.get
                        ((String) opts.get("--layout-file"))));
                Gson g = new GsonBuilder().create();
                oLayout = g.fromJson(layoutJson, Layout.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read from file (not a JSON file?)", e);
            }
        }

        if (oLayout == null) {
            log.trace("Reading layout from stdin.");
            try {
                String s = new String(ByteStreams.toByteArray(System.in));
                Gson g = new GsonBuilder().create();
                oLayout = g.fromJson(s, Layout.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to read from stdin (not valid JSON?)", e);
            }
        }

        log.debug("Parsed layout to {}", oLayout);
        return oLayout;
    }
}
