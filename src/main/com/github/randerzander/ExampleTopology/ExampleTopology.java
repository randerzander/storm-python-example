package com.github.randerzander;

import com.github.randerzander.bolts.PyBolt;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;

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

import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;

import java.io.FileReader;
import java.util.Properties;
import java.util.Enumeration;
import java.util.UUID;

public class ExampleTopology {
  public static void setSpout(Properties props, String spoutName, TopologyBuilder builder){
    String prefix = "topology.spouts." + spoutName + ".";
    SpoutConfig spoutConfig = new SpoutConfig(
      new ZkHosts(props.getProperty(prefix+"zk.host")),
      props.getProperty(prefix+"topic"),
      props.getProperty(prefix+"zk.root"),
      UUID.randomUUID().toString()
    );
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConfig.forceFromStart = props.getProperty(prefix+"forceFromStart").equals("true");
    builder.setSpout(spoutName, new KafkaSpout(spoutConfig));
  }

  public static BoltDeclarer getDeclarer(Properties props, String boltName, TopologyBuilder builder){
    String prefix = "topology.bolts." + boltName + ".";
    String type = props.getProperty(prefix+"type");
    BoltDeclarer declarer = null;
    //TODO parallelism
    //int parallelism = Integer.parseInt(props.getProperty(prefix+"parallelism"));

    if (type.equals("HDFSBolt")){
      HdfsBolt bolt = new HdfsBolt().withFsUrl(props.getProperty(prefix+"withFsUrl"))
        .withFileNameFormat(new DefaultFileNameFormat().withPath(props.getProperty(prefix+"outputDir")))
        .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("\t"))
        .withRotationPolicy(new FileSizeRotationPolicy(256, Units.MB))
        .withSyncPolicy(new CountSyncPolicy(Integer.parseInt(props.getProperty(prefix+"countSyncPolicy"))));
      declarer = builder.setBolt(boltName, bolt);
    }else if (type.equals("PyBolt")){
      declarer = builder.setBolt(boltName, new PyBolt(boltName + ".py", ((String)props.getProperty(prefix+"fields")).split(",")));
    }else if (type.equals("HBaseBolt")){
      String[] fields = ((String)props.getProperty(prefix+"fields")).split(",");
      SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField(props.getProperty(prefix+"rowKeyField"))
        .withColumnFields(new Fields(fields))
        .withColumnFamily(props.getProperty(prefix+"cf"));
      HBaseBolt bolt = new HBaseBolt(props.getProperty(prefix+"table"), mapper)
        .withConfigKey("hbase.properties");
      declarer = builder.setBolt(boltName, bolt);
    }else{
      System.err.println("Invalid bolt type: " + prefix + ": " + type);
      System.exit(-1);
    }
    return declarer;
  }

  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.load(new FileReader(args[0]));

    TopologyBuilder builder = new TopologyBuilder();
    //Spouts and bolts are defined as comma separated entried in topology.properties as topology.spouts and topology.bolts
    for(String spoutName: ((String)props.get("topology.spouts")).split(",")){
      System.out.println("Setting up " + spoutName + " spout");
      setSpout(props, spoutName, builder);
    }
    for(String boltName: ((String)props.get("topology.bolts")).split(",")){
      String prefix = "topology.bolts."+boltName+".";
      String grouping = props.getProperty(prefix+"grouping");
      String source = props.getProperty(prefix+"source");
      if (grouping.equals("shuffle")){
        System.out.println("Setting up " + boltName + " bolt");
        getDeclarer(props, boltName, builder).shuffleGrouping(source);
      }else if (grouping.equals("fields")){
        System.err.println("Fields or other groupings not yet supported.");
        System.exit(-1);
      }
    }

    Config conf = new Config();
    conf.setNumWorkers(Integer.parseInt(props.getProperty("topology.numWorkers")));
    StormSubmitter.submitTopology(props.getProperty("topology.name"), conf, builder.createTopology());
  }
}
