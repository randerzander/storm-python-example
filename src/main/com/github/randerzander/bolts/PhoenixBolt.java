package com.github.randerzander.bolts;

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
  private String jarFile;
  private String fqClassName;
  private Connection connection;
	
	private OutputCollector collector;

	public PhoenixBolt(String _jdbcURL, String _jarFile, String _fqClassName){
    jdbcURL = _jdbcURL; jarFile = _jarFile; fqClassName = _fqClassName;
  }

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector) {
    try{
      URLClassLoader loader = new URLClassLoader(new URL[]{new URL(jarFile)}, null);
      connection = ((Driver) loader.loadClass(fqClassName).newInstance()).connect(jdbcURL, new Properties());
    }catch(Exception e){ e.printStackTrace(); System.exit(-1); }
		
		collector = _collector;
	}

	@Override
	public void execute(Tuple tuple) {
		try
		{
      //TODO: build query string dynamically from Field and Value input
      String statement = "upsert into " + tuple.getStringByField("table") + "(";
      String values = "(";
      for (String field : tuple.getFields()){ 
        if (!field.equals("table")){
          statement += field + ",";
          values += tuple.getStringByField(field) + ",";
        }
      }
      statement = statement.substring(0, statement.length() - 1) + ") values " + values.substring(0, values.length()-1) + ")";
      connection.createStatement().executeQuery(statement);
		}
		catch(Exception e){
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields()); }
	@Override
	public Map<String, Object> getComponentConfiguration() { return null; }
  @Override
  public void cleanup(){}
}
