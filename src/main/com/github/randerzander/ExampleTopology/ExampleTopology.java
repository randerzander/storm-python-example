package com.github.randerzander;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.starter.spout.RandomSentenceSpout;

public class ExampleTopology {
    public static class PyBolt extends ShellBolt implements IRichBolt {
      public PyBolt() { super("python", "example.py"); }

      @Override
      public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("word")); }

      @Override
      public Map<String, Object> getComponentConfiguration() { return null; }
    }
    
    public static void main(String[] args) throws Exception {
      TopologyBuilder builder = new TopologyBuilder();

      //configure spout input
      SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("n0.dev:2181"), args[0], "/kafkastorm", UUID.randomUUID().toString());
      spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
      KafkaSpout spout = new KafkaSpout(spoutConfig);
      builder.setSpout("spout", spout);
      //builder.setSpout("spout", new RandomSentenceSpout());

      //configure processors
      builder.setBolt("bolt", new PyBolt()).shuffleGrouping("spout"); //consumes directly from spout
      Config conf = new Config();
      conf.setNumWorkers(1);

      //launch topology
      StormSubmitter.submitTopology("topology", conf, builder.createTopology());
    }
}
