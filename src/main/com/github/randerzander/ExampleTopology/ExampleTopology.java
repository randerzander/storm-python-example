package com.github.randerzander;

import com.github.randerzander.bolts.PyBolt;
import com.github.randerzander.bolts.PhoenixBolt;
import com.github.randerzander.utils.DateTimeFileNameFormat;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BoltDeclarer;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;

import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.StringScheme;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;

import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;

import java.io.FileReader;
import java.util.Properties;
import java.util.Enumeration;
import java.util.UUID;
import java.util.Map;
import java.util.HashMap;

public class ExampleTopology {
  public static void setSpout(HashMap<String, String> props, String spoutName, TopologyBuilder builder){
    System.out.println("Setting up " + spoutName + " spout");
    String prefix = "spouts." + spoutName + ".";
    SpoutConfig spoutConfig = new SpoutConfig(
      new ZkHosts(props.get(prefix+"zk.host")),
      props.get(prefix+"topic"),
      props.get(prefix+"zk.root"),
      UUID.randomUUID().toString()
    );
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    spoutConfig.forceFromStart = props.get(prefix+"forceFromStart").equals("true");
    builder.setSpout(spoutName, new KafkaSpout(spoutConfig));
  }

  public static BoltDeclarer getDeclarer(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    System.out.println("Setting up " + boltName + " bolt");
    String prefix = "bolts." + boltName + ".";
    String type = props.get(prefix+"type");
    BoltDeclarer declarer = null;
    //TODO parallelism
    //int parallelism = Integer.parseInt(props.get(prefix+"parallelism"));

    if (type.equals("HDFSBolt")){
      HdfsBolt bolt = new HdfsBolt().withFsUrl(props.get(prefix+"withFsUrl"))
        .withFileNameFormat(new DateTimeFileNameFormat().withPath(props.get(prefix+"outputDir")))
        .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("\t"))
        .withRotationPolicy(new FileSizeRotationPolicy(256, Units.MB))
        .withSyncPolicy(new CountSyncPolicy(Integer.parseInt(props.get(prefix+"countSyncPolicy"))));
      declarer = builder.setBolt(boltName, bolt);
    }else if (type.equals("PyBolt")){
      declarer = builder.setBolt(boltName, new PyBolt(boltName + ".py", ((String)props.get(prefix+"fields")).split(",")));
    }else if (type.equals("HBaseBolt")){
      //Use specified fields, else use source bolt's fields
      String fields = null;
      if (props.get(prefix+"fields") != null) fields = props.get(prefix+"fields");
      else fields = props.get("bolts."+props.get(prefix+"source")+".fields");

      SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField(props.get(prefix+"rowKeyField"))
        .withColumnFields(new Fields(fields.split(",")))
        .withColumnFamily(props.get(prefix+"cf"));
      HBaseBolt bolt = new HBaseBolt(props.get(prefix+"table"), mapper);
        //.withConfigKey(props.get(prefix+"configKey")); TODO -- 2.2 upgrade
      declarer = builder.setBolt(boltName, bolt);
    }else if (type.equals("PhoenixBolt")){
      String[] fields = null;
      if (props.get(prefix+"fields") != null) fields = props.get(prefix+"fields").split(",");
      declarer = builder.setBolt(boltName, new PhoenixBolt(
        props.get(prefix+"jdbcJar"), props.get(prefix+"jdbcURL"), props.get(prefix+"table"), fields)
      );
    }else{
      System.err.println("Invalid bolt type: " + prefix + ": " + type);
      System.exit(-1);
    }
    return declarer;
  }

  public static void main(String[] args) throws Exception {
    HashMap<String, String> props = getPropertiesMap(args[0]);
    TopologyBuilder builder = new TopologyBuilder();

    //Set up spouts and bolts
    for(String spoutName: props.get("spouts").split(",")){ setSpout(props, spoutName, builder); }
    for(String boltName: props.get("bolts").split(",")){
      String prefix = "bolts."+boltName+".";
      if (props.get(prefix+"grouping").equals("shuffle")){
        getDeclarer(props, boltName, builder).shuffleGrouping(props.get(prefix+"source"));
      }
      else{ System.err.println("Non shufflegroupings not supported."); System.exit(-1);  }
    }

    if (props.get("killAlreadyRunningTopology").equals("true")){
      Client client = NimbusClient.getConfiguredClient(Utils.readStormConfig()).getClient();
      KillOptions opts = new KillOptions();
      int killWait = Integer.parseInt(props.get("killWait"));
      opts.set_wait_secs(killWait);
      try{ client.killTopologyWithOpts(props.get("topologyName"), opts); Thread.sleep((killWait+5)*1000); }
      catch(Exception e){}
    }

    //Submit topology
    Config conf = new Config();
    conf.setNumWorkers(Integer.parseInt(props.get("numWorkers")));
    StormSubmitter.submitTopology(props.get("topologyName"), conf, builder.createTopology());
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
