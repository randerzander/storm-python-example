package com.github.randerzander.bolts;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Config;

import java.util.Properties;
import java.util.Map;

public class KafkaBolt extends BaseRichBolt{
	private OutputCollector collector;
  private String host;
  private String topic;
  private Producer<String, String> producer;
  private String delim;

  public KafkaBolt(String _host, String _topic, String _delim){ this.host = _host; this.topic = _topic; this.delim=_delim; }

	public void prepare(Map stormConf, TopologyContext context, OutputCollector _collector){
		this.collector = _collector;
    System.err.println(this.host);
    Properties props = new Properties();
    props.put("metadata.broker.list", this.host);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    props.put("request.required.acks", "1");
    producer = new Producer<String, String>(new ProducerConfig(props));
	}

  public void execute(Tuple tuple){
    String message = "";
    for(String field: tuple.getFields()) message += tuple.getStringByField(field) + delim;
    message = message.substring(0, message.length()-1); //remove trailing delim
    producer.send(new KeyedMessage(topic, message));
    collector.ack(tuple);
  }

	public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}
