package com.github.randerzander;

import com.github.randerzander.bolts.Bolts;
import com.github.randerzander.spouts.Spouts;
import com.github.randerzander.Utils;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

import java.io.FileReader;
import java.util.Properties;
import java.util.HashMap;
import java.util.Arrays;

public class DynamicTopology {
  public static void main(String[] args) throws Exception {
    HashMap<String, String> props = getPropertiesMap(args[0]);
    String topologyName = props.get("topologyName");
    if (props.get("killIfRunning").equals("true")) Utils.killTopology(topologyName);

    TopologyBuilder builder = new TopologyBuilder();
    Config conf = new Config();
    conf.setNumWorkers(Integer.parseInt(props.get("numWorkers")));

    //Set up spouts and bolts
    for(String spoutName: props.get("spouts").split(",")){
      System.out.println("Setting up Spout: " + spoutName);
      Spouts.getDeclarer(props, spoutName, builder);
    }
    for(String boltName: props.get("bolts").split(",")){
      System.out.println("Setting up Bolt: " + boltName);
      String prefix = "bolts."+boltName+".";
      if (props.get(prefix+"grouping") == null || props.get(prefix+"grouping").equals("shuffle")){
        Bolts.getDeclarer(props, boltName, builder).shuffleGrouping(props.get(prefix+"source"));
      }
      else{ System.err.println("Non shufflegroupings not supported."); System.exit(-1);  }
    }

    //Submit topology
    StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
  }

  public static HashMap<String, String> getPropertiesMap(String file){
    Properties props = new Properties();
    try{ props.load(new FileReader(file)); }
    catch(Exception e){ e.printStackTrace(); System.exit(-1); }

    HashMap<String, String> map = new HashMap<String, String>();
    for (final String name: props.stringPropertyNames()) map.put(name, (String)props.get(name));
    return map;
  }
}
