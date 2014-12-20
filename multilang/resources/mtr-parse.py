#!/usr/bin/python

import storm, sys, json, datetime

class ExampleBolt(storm.BasicBolt):
  #def initialize(self, stormconf, context):
    # Can define init steps here as needed

  def process(self, tup):
    try: report = json.loads(tup.values[0])
    except ValueError: return # Abort tuple in case of invalid JSON

    src_ip = report['Client']['IP']
    target_ip = report['Command'][5]
    local_time = report['StartTime']
    # Parse output by newlines, get last line
    last_line = report['Output'].split('\x00')[-2]
    if '???' in last_line: return # Don't report on an incomplete route

    tokens = last_line.split()
    loss = float(tokens[2].replace('%', ''))
    stddev = float(tokens[-1])
    avg = float(tokens[5])
    
    storm.emit([src_ip, target_ip, local_time, loss, avg, stddev])

ExampleBolt().run()
