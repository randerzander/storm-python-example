package com.github.randerzander.spouts;
import com.github.randerzander.utils.CSVScheme;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.SpoutDeclarer;

import java.util.HashMap;
import java.util.UUID;

public class Spouts {
  public static SpoutDeclarer getKafkaSpout(HashMap<String, String> props, String spoutName, TopologyBuilder builder){
    String prefix = "spouts." + spoutName + ".";
    SpoutConfig spoutConfig = new SpoutConfig(
      new ZkHosts(props.get(prefix+"zk.host")),
      props.get(prefix+"topic"),
      props.get(prefix+"zk.root"),
      UUID.randomUUID().toString()
    );
    if (props.get(prefix+"scheme") != null && props.get(prefix+"scheme").equals("CSVScheme"))
      spoutConfig.scheme = new SchemeAsMultiScheme(new CSVScheme(getFields(props, spoutName), props.get(prefix+"delim")));
    else spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    
    spoutConfig.forceFromStart = props.get(prefix+"forceFromStart").equals("true");
    return builder.setSpout(spoutName, new KafkaSpout(spoutConfig));
  }

  public static SpoutDeclarer getDeclarer(HashMap<String, String> props, String spoutName, TopologyBuilder builder){
    String prefix = "spouts." + spoutName + ".";
    String type = props.get(prefix+"type");
    if (type.equals("KafkaSpout")) return getKafkaSpout(props, spoutName, builder);
    return null;
  }

  public static String[] getFields(HashMap<String, String> props, String spoutName){
    String prefix = "spouts." + spoutName + ".";
    //Use specified fields, else use source bolt's fields
    String tmp = null;
    if (props.get(prefix+"fields") != null) tmp = props.get(prefix+"fields");
    return (tmp != null) ? tmp.split(",") : null;
  }
}
