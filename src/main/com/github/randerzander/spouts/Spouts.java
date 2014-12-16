package com.github.randerzander.spouts;
import com.github.randerzander.utils.CSVScheme;
import com.github.randerzander.Utils;

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
    String prefix = spoutName + ".";
    SpoutConfig spoutConfig = new SpoutConfig(
      new ZkHosts(props.get(prefix+"zk.host")),
      props.get(prefix+"topic"),
      (props.get(prefix+"zk.root") == null) ? "/kafkastorm" : props.get(prefix+"zk.root"), //use /kafkastorm if no zkroot given
      UUID.randomUUID().toString()
    );
    if (Utils.checkProp(props, prefix+"scheme", "CSVScheme"))
      spoutConfig.scheme = new SchemeAsMultiScheme(new CSVScheme(Utils.getFields(props, spoutName), props.get(prefix+"delim")));
    else spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    
    spoutConfig.forceFromStart = (Utils.checkProp(props, prefix+"fromBeginning", "true"));
    return builder.setSpout(spoutName, new KafkaSpout(spoutConfig), Utils.getParallelism(props, spoutName));
  }

  public static SpoutDeclarer getDeclarer(HashMap<String, String> props, String spoutName, TopologyBuilder builder){
    if (props.get(spoutName+".spoutType").equals("KafkaSpout")) return getKafkaSpout(props, spoutName, builder);
    return null;
  }

}
