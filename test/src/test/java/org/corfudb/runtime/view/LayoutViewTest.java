package org.corfudb.runtime.view;

import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.BooleanArrayAssert;
import org.corfudb.infrastructure.TestLayoutBuilder;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.LayoutCommittedRequest;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.TestClientRouter;
import org.corfudb.runtime.clients.TestRule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by mwei on 1/6/16.
 */
@Slf4j
public class LayoutViewTest extends AbstractViewTest {
    //@Test
    public void canGetLayout() {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = r.getLayoutView().getCurrentLayout();
        assertThat(l.asJSONString())
                .isNotNull();
    }

    //////////////////////@Test
    public void canSetLayout()
            throws Exception {
        CorfuRuntime r = getDefaultRuntime().connect();
        Layout l = new TestLayoutBuilder()
                .setEpoch(1)
                .addLayoutServer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        l.setRuntime(r);
        l.moveServersToEpoch();
        r.getLayoutView().updateLayout(l, 1L);
        r.invalidateLayout();
        assertThat(r.getLayoutView().getLayout().epoch)
                .isEqualTo(1L);
    }

    ///////////////@Test
    public void canTolerateLayoutServerFailure()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        bootstrapAllServers(new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build());

        CorfuRuntime r = getRuntime().connect();

        // Fail the network link between the client and test server
        addServerRule(SERVERS.PORT_1, new TestRule()
                .always()
                .drop());

        r.invalidateLayout();

        r.getStreamsView().get(CorfuRuntime.getStreamID("hi")).check();
    }

    @Test public void notUnanimous0() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous1() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous2() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous3() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous4() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous5() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous6() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous7() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous8() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous9() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous10() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous11() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous12() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous13() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous14() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous15() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous16() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous17() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous18() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous19() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous20() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous21() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous22() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous23() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous24() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous25() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous26() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous27() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous28() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous29() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous30() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous31() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous32() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous33() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous34() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous35() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous36() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous37() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous38() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous39() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous40() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous41() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous42() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous43() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous44() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous45() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous46() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous47() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous48() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous49() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous50() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous51() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous52() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous53() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous54() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous55() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous56() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous57() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous58() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous59() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous60() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous61() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous62() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous63() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous64() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous65() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous66() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous67() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous68() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous69() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous70() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous71() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous72() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous73() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous74() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous75() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous76() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous77() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous78() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous79() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous80() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous81() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous82() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous83() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous84() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous85() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous86() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous87() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous88() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous89() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous90() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous91() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous92() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous93() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous94() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous95() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous96() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous97() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous98() throws Exception { notUnanimousLayoutServers();}
    @Test public void notUnanimous99() throws Exception { notUnanimousLayoutServers();}

    public void notUnanimousLayoutServers()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);

        Layout l0 = new TestLayoutBuilder()
                .setEpoch(0L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_0)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l0);
        Layout l1 = l0;
        l1.setEpoch(1L);
        CorfuMsg setEpoch1Msg = CorfuMsgType.LAYOUT_COMMITTED.payloadMsg(new LayoutCommittedRequest(1L, l1));

        TestClientRouter rx = (TestClientRouter) runtime.getRouter(SERVERS.ENDPOINT_0);
        for (int i = 0; i < 1; i++) {
            Boolean setEpoch1Reply = (Boolean) rx.sendMessageAndGetCompletable(setEpoch1Msg).get();
            System.err.printf("setEpoch1Reply = %s\n", setEpoch1Reply.toString());
        }

        CorfuRuntime corfuRuntime = getRuntime(l0).connect();
        for (int i = 2; i < 2*2*2*2*2; i++) {
            try {
                corfuRuntime.invalidateLayout();
                System.err.printf("e=%d,\n", corfuRuntime.getLayoutView().getLayout().getEpoch());
            } catch (Exception e) {
                System.err.printf("\nBummer, error %s at %s\n", e.toString(), e.getStackTrace());
                throw new ArithmeticException();
            }
        }
        System.err.printf("\n");
    }

    /**
     * Fail a server and reconfigure
     * while data operations are going on.
     * Details:
     * Start with a configuration of 3 servers SERVERS.PORT_0, SERVERS.PORT_1, SERVERS.PORT_2.
     * Perform data operations. Fail SERVERS.PORT_1 and reconfigure to have only SERVERS.PORT_0 and SERVERS.PORT_2.
     * Perform data operations while the reconfiguration is going on. The operations should
     * be stuck till the new configuration is chosen and then complete after that.
     * FIXME: We cannot failover the server with the primary sequencer yet.
     *
     * @throws Exception
     */
    /////////////////////// @Test
    public void reconfigurationDuringDataOperations()
            throws Exception {
        addServer(SERVERS.PORT_0);
        addServer(SERVERS.PORT_1);
        addServer(SERVERS.PORT_2);
        Layout l = new TestLayoutBuilder()
                .setEpoch(1L)
                .addLayoutServer(SERVERS.PORT_0)
                .addLayoutServer(SERVERS.PORT_1)
                .addLayoutServer(SERVERS.PORT_2)
                .addSequencer(SERVERS.PORT_0)
                .addSequencer(SERVERS.PORT_1)
                .addSequencer(SERVERS.PORT_2)
                .buildSegment()
                .buildStripe()
                .addLogUnit(SERVERS.PORT_0)
                .addLogUnit(SERVERS.PORT_1)
                .addLogUnit(SERVERS.PORT_2)
                .addToSegment()
                .addToLayout()
                .build();
        bootstrapAllServers(l);
        CorfuRuntime corfuRuntime = getRuntime(l).connect();

        // Thread to reconfigure the layout
        CountDownLatch startReconfigurationLatch = new CountDownLatch(1);
        CountDownLatch layoutReconfiguredLatch = new CountDownLatch(1);

        Thread t = new Thread(() -> {
            try {
                startReconfigurationLatch.await();
                corfuRuntime.invalidateLayout();

                // Fail the network link between the client and test server
                addServerRule(SERVERS.PORT_1, new TestRule().always().drop());
                // New layout removes the failed server SERVERS.PORT_0
                Layout newLayout = new TestLayoutBuilder()
                        .setEpoch(l.getEpoch() + 1)
                        .addLayoutServer(SERVERS.PORT_0)
                        .addLayoutServer(SERVERS.PORT_2)
                        .addSequencer(SERVERS.PORT_0)
                        .addSequencer(SERVERS.PORT_2)
                        .buildSegment()
                        .buildStripe()
                        .addLogUnit(SERVERS.PORT_0)
                        .addLogUnit(SERVERS.PORT_2)
                        .addToSegment()
                        .addToLayout()
                        .build();
                newLayout.setRuntime(corfuRuntime);
                //TODO need to figure out if we can move to
                //update layout
                newLayout.moveServersToEpoch();

                corfuRuntime.getLayoutView().updateLayout(newLayout, newLayout.getEpoch());
                corfuRuntime.invalidateLayout();
                log.debug("layout updated new layout {}", corfuRuntime.getLayoutView().getLayout());
                layoutReconfiguredLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.start();

        // verify writes and reads happen before and after the reconfiguration
        StreamView sv = corfuRuntime.getStreamsView().get(CorfuRuntime.getStreamID("streamA"));
        // This write will happen before the reconfiguration while the read for this write
        // will happen after reconfiguration
        writeAndReadStream(corfuRuntime, sv, startReconfigurationLatch, layoutReconfiguredLatch);
        // Write and read after reconfiguration.
        writeAndReadStream(corfuRuntime, sv, startReconfigurationLatch, layoutReconfiguredLatch);
        t.join();
    }

    private void writeAndReadStream(CorfuRuntime corfuRuntime, StreamView sv, CountDownLatch startReconfigurationLatch, CountDownLatch layoutReconfiguredLatch) throws InterruptedException {
        byte[] testPayload = "hello world".getBytes();
        sv.write(testPayload);
        startReconfigurationLatch.countDown();
        layoutReconfiguredLatch.await();
        assertThat(sv.read().getPayload(corfuRuntime)).isEqualTo("hello world".getBytes());
        assertThat(sv.read()).isEqualTo(null);
    }

}
