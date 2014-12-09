package com.github.randerzander.bolts;

import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

public class PyBolt extends ShellBolt implements IRichBolt {
  String[] fields;
  public PyBolt(String filename, String[] _fields) {
    super("python", filename);
    fields = _fields;
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields(fields));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
