package com.github.randerzander.bolts;

import java.io.File;

import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
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
  private String table;
  private String[] fields;
	
  private OutputCollector collector;

  public PhoenixBolt(String _jdbcJar, String _jdbcURL, String _table, String[] _fields){
    jdbcJar = _jdbcJar;
    jdbcURL = _jdbcURL;
    table = _table;
    fields = _fields;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector){
    collector = _collector;
    try{
      URLClassLoader loader = new URLClassLoader(new URL[]{new URL(jdbcJar)}, null);
      Driver driver = (Driver) loader.loadClass("org.apache.phoenix.jdbc.PhoenixDriver").newInstance();
      Connection connection = driver.connect(jdbcURL, new Properties());
    }catch(Exception e){ e.printStackTrace(); System.exit(-1); }
  }

  @Override
  public void execute(Tuple tuple) {
    try{
      String values = "(";
      String statement = "upsert into " + table + "(";
      //Use fields if specified, else use whatever specified by upstream bolt
      if (fields != null) for(String field: fields) statement += "," + field;
      for (String field : tuple.getFields()){ 
        if (fields != null) statement += field + ",";
        values += tuple.getStringByField(field) + ",";
      }
      statement = statement.substring(0, statement.length() - 1) + ") values (" + values.substring(0, values.length()-1) + ")";
      connection.createStatement().executeQuery(statement);
    }catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields()); }
  @Override
  public Map<String, Object> getComponentConfiguration() { return null; }
  @Override
  public void cleanup(){}
}
