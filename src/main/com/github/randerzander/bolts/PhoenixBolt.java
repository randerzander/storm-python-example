package com.github.randerzander.bolts;

import java.io.File;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class PhoenixBolt implements IRichBolt {
  private String jdbcURL;
  private String jdbcJar;
  private Connection connection;
  private OutputCollector collector;

  public PhoenixBolt(String _jdbcJar, String _jdbcURL){
    jdbcJar = _jdbcJar;
    jdbcURL = _jdbcURL;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector){
    collector = _collector;
    try{
      URLClassLoader loader = new URLClassLoader(new URL[]{new URL(jdbcJar)}, null);
      Driver driver = (Driver) loader.loadClass("org.apache.phoenix.jdbc.PhoenixDriver").newInstance();
      connection = driver.connect(jdbcURL, new Properties());
    }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
  }

  @Override
  public void execute(Tuple tuple){
    try{
      connection.createStatement().executeUpdate(tuple.getStringByField("statement"));
      connection.commit();
    }
    catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer){}
  @Override
  public Map<String, Object> getComponentConfiguration() { return null; }
  @Override
  public void cleanup(){}
}
