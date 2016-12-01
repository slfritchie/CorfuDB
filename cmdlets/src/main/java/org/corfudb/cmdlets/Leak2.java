package org.corfudb.cmdlets;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.FGMap;
import org.corfudb.runtime.collections.SMRMap;

import java.util.Map;
import java.util.Set;

/**
 * Created by dmalkhi on 11/15/16, slfritchie on 11/16/16.
 */
public class Leak2 {
  private static CorfuRuntime getRuntimeAndConnect(String configurationString) {
    CorfuRuntime corfuRuntime = new CorfuRuntime(configurationString).connect();
    return corfuRuntime;
  }

  /*
[{[{init,{state,smrmap,"sfritchie-m01:8000",
                [cmdlet0,cmdlet1,cmdlet2,cmdlet3,cmdlet4,cmdlet5,cmdlet6,
                 cmdlet7,cmdlet8,cmdlet9],
                false,42,[]}}],
  [[],
   [{set,{var,1},
         {call,map_qc,reset,
               [{cmdlet0,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000"]}},
    {set,{var,2},
         {call,map_qc,put,
               [{cmdlet4,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"x",
                "fsmgmhdpttnvhsilbtnnjzdctggu"]}},
    {set,{var,3},
         {call,map_qc,remove,
               [{cmdlet1,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"a"]}},
    {set,{var,4},
         {call,map_qc,remove,
               [{cmdlet7,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"a"]}},
    {set,{var,5},
         {call,map_qc,isEmpty,
               [{cmdlet3,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap]}},
    {set,{var,6},
         {call,map_qc,put,
               [{cmdlet6,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"t","Hello-world!"]}},
    {set,{var,7},
         {call,map_qc,get,
               [{cmdlet5,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"a"]}},
    {set,{var,8},
         {call,map_qc,isEmpty,
               [{cmdlet2,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap]}},
    {set,{var,9},
         {call,map_qc,remove,
               [{cmdlet8,'corfu-8000@sfritchie-m01'},
                "sfritchie-m01:8000",42,smrmap,"a"]}}]]}]
   */
  public static void main(String[] args) {
    try {
      String endpoint = args[0];
      String streamName = args[1];
      int iters = Integer.parseInt(args[2]);

      for (int i = 0; i < iters; i++) {
        CorfuRuntime runtime4 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap4 = runtime4.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        String var2 = testMap4.put("x", "fsmgmhdpttnvhsilbtnnjzdctggu");

        CorfuRuntime runtime1 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap1 = runtime1.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        String var3 = testMap1.remove("a");

        CorfuRuntime runtime7 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap7 = runtime7.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        String var4 = testMap7.remove("a");

        CorfuRuntime runtime3 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap3 = runtime3.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        Boolean var5 = testMap3.isEmpty();

        CorfuRuntime runtime6 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap6 = runtime6.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        String var6 = testMap6.put("t", "Hello-world!");

        CorfuRuntime runtime5 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap5 = runtime5.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        String var7 = testMap5.get("a");

        CorfuRuntime runtime2 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap2 = runtime2.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        Boolean var8 = testMap2.isEmpty();

        CorfuRuntime runtime8 = getRuntimeAndConnect(endpoint);
        Map<String, String> testMap8 = runtime8.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();
        String var9 = testMap8.remove("a");


        cleanup(runtime4);
        cleanup(runtime1);
        cleanup(runtime7);
        cleanup(runtime3);
        cleanup(runtime6);
        cleanup(runtime5);
        cleanup(runtime2);
        cleanup(runtime8);
      }
    } catch (Exception e) {
      System.out.printf("Oops, exception %s\n", e.toString());
    }
  }

  public static void cleanup(CorfuRuntime rt) {
      // Brrrrr, state needs resetting in rt's ObjectsView
      rt.getObjectsView().getObjectCache().clear();
      // Brrrrr, state needs resetting in rt's AddressSpaceView
      rt.getAddressSpaceView().resetCaches();
      // Stop the router, sortof.  false means don't really shutdown,
      // but disconnect any existing connection.
      rt.stop(false);
  }
}

