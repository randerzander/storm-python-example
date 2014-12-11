package com.github.randerzander.bolts;

import com.github.randerzander.Utils;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.DBCursor;

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
  private final String collectionName;
  private final String dbName;
  private final String url;
  private final String user;
  private final String password;
  private String[] fields;
  private MongoClient client;
  private DB db;
  private DBCollection collection;

  private OutputCollector collector;

  public MongoBolt(String _user, String _password, String url, String dbName, String collectionName, String[] _fields){
    this.user = _user;
    this.password = _password;
    this.url = url;
    this.dbName = dbName;
    this.collectionName = collectionName;
    this.fields = _fields;
  }
  
  public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector) {
    try{
      if (user != null && password != null){
        MongoCredential cred = MongoCredential.createMongoCRCredential(user, dbName, password.toCharArray());
        client = new MongoClient(new ServerAddress(url), java.util.Arrays.asList(cred));
      }
      else client = new MongoClient(new ServerAddress(url));
      db = client.getDB(dbName);
      collection = db.getCollection(collectionName);
    }catch (UnknownHostException e){ e.printStackTrace(); System.exit(-1); }
    collector = _collector;
  }

  public void execute(Tuple tuple){
    //New incoming parsed record, increment count of this request
    BasicDBObject search = new BasicDBObject();
    if (fields != null) for (String field: fields)
      search.append(field, tuple.getStringByField(field));
    else for (String field: tuple.getFields())
      search.append(field, tuple.getStringByField(field));

    BasicDBObject updated = new BasicDBObject();
    //TODO: make this bolt generic by passing commands as params instead of hardcoded
      updated.put("$inc", new BasicDBObject().append("hits", 1).append("bytes", Integer.parseInt(tuple.getStringByField("bytes"))));
      collection.update(search, updated, true, false);
    //DBCursor cursor = collection.find(search);
    //while(cursor.hasNext()) System.out.println(cursor.next());

    collector.ack(tuple);
  }

  public void declareOutputFields(OutputFieldsDeclarer declarer){}
}
