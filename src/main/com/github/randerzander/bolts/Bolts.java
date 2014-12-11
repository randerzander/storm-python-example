package com.github.randerzander.bolts;
import com.github.randerzander.bolts.PyBolt;
import com.github.randerzander.bolts.PhoenixBolt;
import com.github.randerzander.bolts.MongoBolt;
import com.github.randerzander.bolts.KafkaBolt;

import backtype.storm.tuple.Fields;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

/*
TODO: use 2.2 KafkaBolt
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
*/

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
  public static BoltDeclarer getMongoBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
   String prefix = "bolts." + boltName + ".";
    return builder.setBolt(boltName, new MongoBolt(props.get(prefix+"user"), props.get(prefix+"password"),
      props.get(prefix+"host"), props.get(prefix+"db"), props.get(prefix+"collection"), getFields(props,boltName))
    );
  }

  public static BoltDeclarer getHBaseBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = "bolts." + boltName + ".";
    SimpleHBaseMapper mapper = new SimpleHBaseMapper().withRowKeyField(props.get(prefix+"rowKeyField"))
      .withColumnFields(new Fields(getFields(props,boltName)))
      .withColumnFamily(props.get(prefix+"cf"));
    HBaseBolt bolt = new HBaseBolt(props.get(prefix+"table"), mapper);
      //.withConfigKey(props.get(prefix+"configKey")); TODO -- 2.2 upgrade
    return builder.setBolt(boltName, bolt);
  }

  public static BoltDeclarer getKafkaBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = "bolts." + boltName + ".";
    KafkaBolt bolt = new KafkaBolt(props.get(prefix+"brokerhost"), props.get(prefix+"topic"), props.get(prefix+"delim"));
    //  .withTopicSelector(new DefaultTopicSelector(props.get(prefix+"topic")))
    //  .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
    return builder.setBolt(boltName, bolt);
  }

  public static BoltDeclarer getHDFSBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = "bolts." + boltName + ".";
    HdfsBolt bolt = new HdfsBolt().withFsUrl(props.get(prefix+"withFsUrl"))
      .withFileNameFormat(new DateTimeFileNameFormat().withPath(props.get(prefix+"outputDir")))
      .withRecordFormat(new DelimitedRecordFormat().withFieldDelimiter("\t"))
      .withRotationPolicy(new FileSizeRotationPolicy(256, Units.MB))
      .withSyncPolicy(new CountSyncPolicy(Integer.parseInt(props.get(prefix+"countSyncPolicy"))));
    return builder.setBolt(boltName, bolt);
  }

  public static BoltDeclarer getPyBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = "bolts." + boltName + ".";
    return builder.setBolt(boltName, new PyBolt(boltName + ".py", getFields(props, boltName)));
  }
  
  public static BoltDeclarer getPhoenixBolt(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    String prefix = "bolts." + boltName + ".";
    return builder.setBolt(boltName, new PhoenixBolt(
      props.get(prefix+"jdbcJar"), props.get(prefix+"jdbcURL"), props.get(prefix+"table"), getFields(props, boltName))
    );
  }

  public static String[] getFields(HashMap<String, String> props, String boltName){
    String prefix = "bolts." + boltName + ".";
    //Use specified fields, else use source bolt's fields
    String tmp = null;
    if (props.get(prefix+"fields") != null) tmp = props.get(prefix+"fields");
    else tmp = props.get("bolts."+props.get(prefix+"source")+".fields");
    return (tmp != null) ? tmp.split(",") : null;
  }

  public static BoltDeclarer getDeclarer(HashMap<String, String> props, String boltName, TopologyBuilder builder){
    //TODO parallelism
    //int parallelism = Integer.parseInt(props.get(prefix+"parallelism"));
    String prefix = "bolts." + boltName + ".";
    String type = props.get(prefix+"type");
    if (type.equals("HDFSBolt")) return getHDFSBolt(props, boltName, builder);
    else if (type.equals("PyBolt")) return getPyBolt(props, boltName, builder);
    else if (type.equals("MongoBolt")) return getMongoBolt(props, boltName, builder);
    else if (type.equals("KafkaBolt")) return getKafkaBolt(props, boltName, builder);
    else if (type.equals("HBaseBolt")) return getHBaseBolt(props, boltName, builder);
    else if (type.equals("PhoenixBolt")) return getPhoenixBolt(props, boltName, builder);
    System.err.println("Invalid bolt type: " + prefix + ": " + type);
    return null;
  }
}
