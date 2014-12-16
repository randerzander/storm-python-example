package com.github.randerzander.bolts;

import backtype.storm.tuple.Fields;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.Config;

import java.util.Map;
import java.util.ArrayList;

public class PyBolt extends ShellBolt implements IRichBolt {
  private ArrayList<String> fields;
  private int tickFrequency = -1;
  public PyBolt(String filename, String[] _fields, String _tickFrequency) {
    super("python", filename);
    fields = new ArrayList<String>();
    for(String field: _fields) fields.add(field);
    if (_tickFrequency != null) tickFrequency = Integer.parseInt(_tickFrequency);
  }
  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) { declarer.declare(new Fields(fields)); }
  @Override
  public Map<String, Object> getComponentConfiguration(){
    if (tickFrequency != -1){
      Config conf = new Config();
      conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequency);
      return conf;
    }
    return null;
  }
}
