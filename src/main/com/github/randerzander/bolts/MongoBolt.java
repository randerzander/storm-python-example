package com.github.randerzander.bolts;

import com.github.randerzander.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import java.net.UnknownHostException;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Constants;

public class MongoBolt extends BaseRichBolt{
  //Mongo connection vars
  private final String collectionName;
  private final String dbName;
  private final String url;
  private MongoClient client;
  private DB db;
  private DBCollection collection;

	private OutputCollector collector;

  public MongoBolt(String url, String dbName, String collectionName) {
    this.url = url;
    this.dbName = dbName;
    this.collectionName = collectionName;
  }
  
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    try{ 
      client = new MongoClient(url);
      db = client.getDB(dbName);
      collection = db.getCollection(collectionName);
    }catch (UnknownHostException e){
      e.printStackTrace();
      System.exit(-1);
    }
	}

	public void execute(Tuple tuple) {
    //New incoming parsed record, increment count of this request
    BasicDBObject search = new BasicDBObject()
      .append("src_ip", tuple.getString(0))
      .append("dst_ip", tuple.getString(1))
      .append("dst_port", tuple.getString(2));
    BasicDBObject updated = new BasicDBObject();
    updated.put("$inc", new BasicDBObject().append("hits", 1).append("bytes", tuple.getInteger(3)));
    collection.update(search, updated, true, false);
    collector.ack(tuple);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields()); }
}
