#!/usr/bin/python

import storm, sys, json, datetime

class ExampleBolt(storm.BasicBolt):
  #def initialize(self, stormconf, context):
    # Can define init steps here as needed

  def quote(text): return '\"' + text + '\"'

  def process(self, tup):
    report = json.loads(tup.values[0])
    src_ip = report['Client']['IP']
    target_ip = report['Command'][5]
    #local_timestamp = str(datetime.datetime.fromtimestamp(int(report['StartTime'])))
    local_time = int(report['StartTime'])*1000
    # Parse output by newlines, get last line
    last_line = report['Output'].split('\x00')[-2]
    if '???' in last_line: return # Don't report on an incomplete route

    tokens = last_line.split()
    loss = tokens[2].replace('%', '')
    stddev = tokens[-1]
    avg = tokens[5]
    
    storm.emit([quote(src_ip), quote(target_ip), local_time, loss, avg, stddev])

ExampleBolt().run()
