package com.github.randerzander.bolts;

import com.github.randerzander.Utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Arrays;
import java.io.File;

import org.apache.commons.lang3.StringUtils;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.Config;

import org.apache.phoenix.jdbc.PhoenixDriver;

public class PhoenixBolt implements IRichBolt {
  private String jdbcURL;
  private Connection connection;
  private OutputCollector collector;
  private HashMap<String, StreamHandler> handlers;
  private HashMap<String, String> sourcesToTables;
  private HashMap<String, String[]> fieldLists;
  private int tickFrequency = -1;
  private int countSynchPolicy = -1;
  private int counter = 0;

  public PhoenixBolt(String jdbcURL){
    this.jdbcURL = jdbcURL;
    sourcesToTables = new HashMap<String, String>();
    fieldLists = new HashMap<String, String[]>();
  }
  
  public PhoenixBolt withTableOutput(String table, String source, String[] fields){
    sourcesToTables.put(source, table);
    fieldLists.put(source, fields);
    return this;
  }

  public PhoenixBolt withCountSynchPolicy(int count){
    this.countSynchPolicy = count;
    return this;
  }

  public PhoenixBolt withTimeSynchPolicy(int seconds){
    this.tickFrequency = seconds;
    return this;
  }

  private class StreamHandler{
    private HashMap<Integer, String> tupleFieldTypes;
    public PreparedStatement statement;

    public StreamHandler(Tuple tuple){
      String source = tuple.getSourceComponent();
      String table = sourcesToTables.get(source).toUpperCase();
      String catalogue = (table.contains(".")) ? table.split("\\.")[0] : null;
      Fields tupleFields = tuple.getFields();

      //Look up fields specified in constructor, else use every field in tuple
      String[] fields = (fieldLists.get(source) != null) ? 
          fieldLists.get(source) :
          tupleFields.toList().toArray(new String[tupleFields.size()]);
      String[] qs = new String[fields.length];
      tupleFieldTypes = new HashMap<Integer, String>();
      //Determine type of each field -- build mapping of field types against first tuple
      for(int i = 0; i < fields.length; i++){
        tupleFieldTypes.put(i, getType(tuple.getValueByField(fields[i])));
        qs[i] = "?";
      }
      try{
        statement = connection.prepareStatement("upsert into " + table + " (" + StringUtils.join(fields, ",") + ") values (" + StringUtils.join(qs, ",") + ")");
        /* TODO: create table if not exists
        table = (table.contains(".")) ? table.split("\\.")[1] : table;
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet results = metaData.getTables(null, null, null, new String[]{"TABLE"});
        boolean tableFound = false;
        System.err.println(catalogue + " , " + table);
        while (results.next() && !tableFound) if (results.getString("TABLE_CAT").equals(catalogue) && results.getString("TABLE_NAME").equals(table)) tableFound = true;
        if (!tableFound) throw new RuntimeException(table + " does not exist!");
        */
      }catch(SQLException e){ e.printStackTrace(); throw new RuntimeException(e); }
    }

    private String getType(Object o){
      if (o instanceof String) return "String";
      else if (o instanceof Integer) return "Integer";
      else if (o instanceof Long) return "Long";
      else if (o instanceof Double) return "Double";
      else if (o instanceof Float) return "Float";
      else return null;
    }

    public void addBatch(Tuple tuple){
      try{
        //Uses field mapping built during construction of this StreamHandler
        //TODO: check if this is actually faster than applying instance of to every field in every tuple
        for (Map.Entry<Integer, String> entry: tupleFieldTypes.entrySet()){
          Integer fieldIndex = entry.getKey(); Integer columnIndex = fieldIndex + 1;
          if (entry.getValue().equals("String")) statement.setString(columnIndex, tuple.getString(fieldIndex));
          else if (entry.getValue().equals("Integer")) statement.setInt(columnIndex, tuple.getInteger(fieldIndex));
          else if (entry.getValue().equals("Long")) statement.setLong(columnIndex, tuple.getLong(fieldIndex));
          else if (entry.getValue().equals("Double")) statement.setDouble(columnIndex, tuple.getDouble(fieldIndex));
          else if (entry.getValue().equals("Float")) statement.setFloat(columnIndex, tuple.getFloat(fieldIndex));
        }
        statement.addBatch();
      }catch(SQLException e){ e.printStackTrace(); throw new RuntimeException(e); }
    }
  }

  public void synch(){
    try{
      for (Map.Entry<String, StreamHandler> entry: handlers.entrySet()) entry.getValue().statement.executeBatch();
      connection.commit();
    }catch(SQLException e){ e.printStackTrace(); throw new RuntimeException(e); }
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector){
    this.collector = collector;
    this.handlers = new HashMap<String, StreamHandler>();
    try{ connection = DriverManager.getConnection(jdbcURL, new Properties()); }
    catch(Exception e){ e.printStackTrace(); throw new RuntimeException(e); }
  }

  @Override
  public void execute(Tuple tuple){
    if (Utils.isTickTuple(tuple)) synch();
    else{
      //TODO: add support for multiple streamIds -- currently assumes single stream
      if (handlers.get(tuple.getSourceComponent()) == null) handlers.put(tuple.getSourceComponent(), new StreamHandler(tuple));
      StreamHandler handler = handlers.get(tuple.getSourceComponent());
      handler.addBatch(tuple);
      if (counter++ % countSynchPolicy == 0) synch();
    }
    collector.ack(tuple);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer){}
  @Override
  public void cleanup(){}
  @Override
  public Map<String, Object> getComponentConfiguration() { 
    if (tickFrequency != -1){
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequency);
      return conf;
    } else return null;
  }
}
