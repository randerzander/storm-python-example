package com.github.randerzander.bolts;

import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;
import java.util.ArrayList;

public class PyBolt extends ShellBolt implements IRichBolt {
  private ArrayList<String> fields;
  public PyBolt(String filename, String[] _fields) {
    super("python", filename);
    fields = new ArrayList<String>();
    for(String field: _fields) fields.add(field);
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields(fields)); }
  @Override
  public Map<String, Object> getComponentConfiguration() { return null; }
}
