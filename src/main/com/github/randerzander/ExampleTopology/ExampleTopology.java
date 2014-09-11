package com.github.randerzander;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.SchemeAsMultiScheme;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;

import java.util.Map;
import java.util.UUID;

public class ExampleTopology {
  public static class PyBolt extends ShellBolt implements IRichBolt {
    public PyBolt() { super("python", "example.py"); }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields("word")); }

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
  }
    
  public static void main(String[] args) throws Exception {
    //configure spout input
    SpoutConfig spoutConfig = new SpoutConfig(new ZkHosts("n0.dev:2181"), args[0], "/kafkastorm", UUID.randomUUID().toString());
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout spout = new KafkaSpout(spoutConfig);
    
    //configure HDFS output
    HdfsBolt hdfsBolt = new HdfsBolt().withFsUrl("hdfs://n0.dev:8020")
      .withFileNameFormat(new DefaultFileNameFormat().withPath("/user/dev/storm-staging"))
      .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("\t"))
      .withRotationPolicy(new FileSizeRotationPolicy(5.0f, Units.MB))
      .withSyncPolicy(new CountSyncPolicy(5)); //synch buffer with HDFS every 5 tuples

    //build & launch topology
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout", spout);
    builder.setBolt("py-bolt", new PyBolt()).shuffleGrouping("kafka-spout");
    builder.setBolt("hdfs-bolt", hdfsBolt).shuffleGrouping("py-bolt");
    Config conf = new Config();
    conf.setNumWorkers(1);
    StormSubmitter.submitTopology("topology", conf, builder.createTopology());
  }
}
