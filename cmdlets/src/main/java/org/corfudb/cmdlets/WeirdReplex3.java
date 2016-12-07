package org.corfudb.cmdlets;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.Layout;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Exchanger;
import org.codehaus.plexus.util.ExceptionUtils;

/**
 * Created by dmalkhi on 11/15/16, slfritchie on 11/16/16.
 */
public class WeirdReplex3 {
    private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
        CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
        return corfuRuntime;
    }

    public static void main(String[] args) {
        try {
            String endpoint = args[0];
            String streamName = args[1];
            String layoutType = args[2];

            CorfuRuntime rtSetup = getRuntimeAndConnect(endpoint);
            String jsonReplex = "{\"layoutServers\":[\"" + endpoint + "\"],\"sequencers\":[\"" + endpoint + "\"],\"segments\":[{\"replicationMode\":\"REPLEX\",           \"start\":0,\"end\":-1,\"stripes\":[{\"logServers\":[\"" + endpoint + "\"]}],\"replexes\":[{\"logServers\":[\"" + endpoint + "\"]}]}],\"epoch\":3}";
            String jsonCR     = "{\"layoutServers\":[\"" + endpoint + "\"],\"sequencers\":[\"" + endpoint + "\"],\"segments\":[{\"replicationMode\":\"CHAIN_REPLICATION\",\"start\":0,\"end\":-1,\"stripes\":[{\"logServers\":[\"" + endpoint + "\"]}]                                                       }],\"epoch\":3}";
            String json;
            if (layoutType.contentEquals("replex")) {
                layoutType = "replex";
                json = jsonReplex;
            } else {
                layoutType = "chain_replication";
                json = jsonCR;
            }
            System.out.printf("JSON for our new epoch:\n%s\n", json);
            Layout replexLayout = Layout.fromJSONString(json);
            try {
                replexLayout.setRuntime(rtSetup);
                replexLayout.moveServersToEpoch();
                rtSetup.getLayoutView().committed(3L, replexLayout);
                rtSetup.invalidateLayout();
                System.out.printf("rtSetup getLayout: %s\n", rtSetup.getLayoutView().getLayout().toString());
                System.out.printf("rtSetup getCurrentLayout: %s\n", rtSetup.getLayoutView().getCurrentLayout().toString());
            } catch (Exception e) {
                System.err.printf("Bummer: updateLayout: %s\n", e.toString());
                System.err.printf("Bummer: updateLayout: %s\n", ExceptionUtils.getStackTrace(e).toString());
                System.exit(1);
            }

            CorfuRuntime runtime1 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap1 = runtime1.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            testMap1.clear();

            String var3 = testMap1.put("a","c");

            CorfuRuntime runtime0 = getRuntimeAndConnect(endpoint);
            Map<String,String> testMap0 = runtime0.getObjectsView().build().setType(SMRMap.class).setStreamName(streamName).open();
            String var4 = testMap0.put("a","d");

            String var5 = testMap1.put("a","e");

            String var6 = testMap1.put("a","f");

            String var7 = testMap0.put("a","g");

            String var8 = testMap0.put("a","h");

            String var9 = testMap1.put("a","i");

            String var10 = testMap1.put("a","j");

            String var11 = testMap0.put("a","k");

            String var12 = testMap1.put("a","l");

            String var13 = testMap0.put("a","m");

            testMap0.clear(); // var14's return value is void
            // Uncomment this line to alter value of var18: String var13b = testMap0.get("a"); System.out.printf("After clear, value %s found\n", var13b);

            String var15 = testMap1.put("a","o");

            String var16 = testMap1.put("a","p");

            String var17 = testMap1.put("a","q");

            String var18 = testMap0.put("a","r");

            if (var18 != null && var18.equals("q")) {
                System.out.printf("Correct value \"%s\" found using type %s\n", var18, layoutType);
                System.exit(0);
            } else {
                System.out.printf("Stale value \"%s\" found using type %s, error!\n", var18, layoutType);
                System.exit(1);
            }
        } catch (Exception e) {
            System.out.printf("Oops, exception %s\n", e.toString());
        }
    }
}

