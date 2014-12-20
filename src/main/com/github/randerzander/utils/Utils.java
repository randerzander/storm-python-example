package com.github.randerzander;

import backtype.storm.Constants;
import backtype.storm.tuple.Tuple;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;

import java.util.HashMap;

public class Utils{
  public static boolean isTickTuple(Tuple tuple) {
    return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
  }

  public static void killTopology(String topologyName){
    Client client = NimbusClient.getConfiguredClient(backtype.storm.utils.Utils.readStormConfig()).getClient();
    if (topologyRunning(client, topologyName)){
      System.out.print(topologyName + " is currently running. Killing..");
      try{
        KillOptions opts = new KillOptions();
        opts.set_wait_secs(0);
        client.killTopologyWithOpts(topologyName, opts);
      }catch(Exception e){}
      while (topologyRunning(client, topologyName)){
        System.out.print(".");
        try{ Thread.sleep(1000); } catch(Exception e){}
      }
      System.out.println("killed.");
    }
  }

  public static boolean topologyRunning(Client client, String topologyName){
    try{
    for (TopologySummary topology: client.getClusterInfo().get_topologies())
      if (topology.get_name().equals(topologyName)) return true;
    }catch(Exception e){ e.printStackTrace(); }
    return false;
  }

  public static String[] getFields(HashMap<String, String> props, String componentName){
    String prefix = componentName + ".";
    String tmp = null;
    if (props.get(prefix+"fields") != null) tmp = props.get(prefix+"fields");
    return (tmp != null) ? tmp.split(",") : null;
  }

  public static boolean checkProp(HashMap<String, String> props, String prop, String val){
    if ((val == null) && (props.get(prop) == null)) return true;
    if ((val != null) && (props.get(prop) == null)) return false;
    if ((val != null) && (props.get(prop).equals(val))) return true;
    return false;
  }

  public static int getParallelism(HashMap<String, String> props, String componentName){
    String prefix = componentName + ".";
    if (props.get(prefix+"parallelism") != null) return Integer.parseInt(props.get(prefix+"parallelism"));
    else return 1;
  }
}
