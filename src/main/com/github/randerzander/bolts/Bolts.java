package com.github.randerzander.bolts;
import com.github.randerzander.bolts.PyBolt;
import com.github.randerzander.bolts.PhoenixBolt;

import com.github.randerzander.Utils;

import backtype.storm.tuple.Fields;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import com.github.randerzander.utils.DateTimeFileNameFormat;

import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.hbase.bolt.HBaseBolt;

import java.util.HashMap;

public class Bolts {
  public static BoltDeclarer getHBaseBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = boltName + ".";
    SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField(props.get(prefix+"rowKeyField"))
      .withColumnFields(new Fields(getFields(props,boltName)))
      .withColumnFamily(props.get(prefix+"cf"));
    HBaseBolt bolt = new HBaseBolt(props.get(prefix+"table"), mapper);
      //.withConfigKey(props.get(prefix+"configKey")); TODO -- 2.2 upgrade
    return builder.setBolt(boltName, bolt, Utils.getParallelism(props, boltName));
  }

  public static BoltDeclarer getKafkaBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = boltName + ".";
    KafkaBolt bolt = new KafkaBolt()
      .withTopicSelector(new DefaultTopicSelector(props.get(prefix+"topic")))
      .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
    return builder.setBolt(boltName, bolt, Utils.getParallelism(props, boltName));
  }

  public static BoltDeclarer getHDFSBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = boltName + ".";
    HdfsBolt bolt = new HdfsBolt().withFsUrl(props.get(prefix+"withFsUrl"))
      .withFileNameFormat(new DateTimeFileNameFormat().withPath(props.get(prefix+"outputDir")))
      .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("\t"))
      .withRotationPolicy(new FileSizeRotationPolicy(256, Units.MB))
      .withSyncPolicy(new CountSyncPolicy(Integer.parseInt(props.get(prefix+"countSyncPolicy"))));
    return builder.setBolt(boltName, bolt, Utils.getParallelism(props, boltName));
  }

  public static BoltDeclarer getPyBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = boltName + ".";
    String script = (props.get(prefix+"script") != null) ? props.get(prefix+"script") : boltName + ".py";
    return builder.setBolt(boltName, new PyBolt(script, getFields(props, boltName), props.get(prefix+"tickFrequency")), Utils.getParallelism(props, boltName));
  }
  
  public static BoltDeclarer getPhoenixBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = boltName + ".";
    return builder.setBolt(boltName, new PhoenixBolt(
      props.get(prefix+"jdbcJar"), props.get(prefix+"jdbcURL"), props.get(prefix+"table"), getFields(props, boltName)),
      Utils.getParallelism(props, boltName)
    );
  }

  public static String[] getFields(HashMap<String, String> props, String boltName){
    String prefix = boltName + ".";
    //Use specified fields, else use source bolt's fields
    String tmp = null;
    if (props.get(prefix+"fields") != null) tmp = props.get(prefix+"fields");
    else tmp = props.get(props.get(prefix+"source")+".fields");
    return (tmp != null) ? tmp.split(",") : null;
  }

  public static BoltDeclarer getDeclarer(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    //TODO parallelism
    //int parallelism = Integer.parseInt(props.get(prefix+"parallelism"));
    String prefix = boltName + ".";
    String type = props.get(prefix+"boltType");
    if (type.equals("HDFSBolt")) return getHDFSBolt(props, boltName, builder);
    else if (type.equals("PyBolt")) return getPyBolt(props, boltName, builder);
    else if (type.equals("KafkaBolt")) return getKafkaBolt(props, boltName, builder);
    else if (type.equals("HBaseBolt")) return getHBaseBolt(props, boltName, builder);
    else if (type.equals("PhoenixBolt")) return getPhoenixBolt(props, boltName, builder);
    System.err.println("Invalid bolt type: " + prefix + ": " + type);
    return null;
  }
}
