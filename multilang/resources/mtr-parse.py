#!/usr/bin/python

import storm, sys, json, datetime

#TODO remove once PhoenixBolt responsible for building queries
def prep(values):
  for idx, val in enumerate(values):
    if type(val) is str or type(val) is unicode: values[idx] = '\''+val+'\''
    elif type(val) is int or type(val) is float: values[idx] = str(val)
  return values

class ExampleBolt(storm.BasicBolt):
  #def initialize(self, stormconf, context):
    # Can define init steps here as needed

  def process(self, tup):
    try: report = json.loads(tup.values[0])
    except ValueError: return # Abort tuple in case of invalid JSON

    src_ip = report['Client']['IP']
    target_ip = report['Command'][5]
    local_time = int(report['StartTime'])
    # Parse output by newlines, get last line
    last_line = report['Output'].split('\x00')[-2]
    if '???' in last_line: return # Don't report on an incomplete route

    tokens = last_line.split()
    loss = float(tokens[2].replace('%', ''))
    stddev = float(tokens[-1])
    avg = float(tokens[5])
    
    values = prep([src_ip, target_ip, local_time, loss, avg, stddev])
    #TODO make PhoenixBolt built upsert statements
    query = 'upsert into mtr.mtr values (' + ','.join(values) + ')'
    storm.emit([query])

ExampleBolt().run()
