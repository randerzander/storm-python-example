#!/usr/bin/python

import sys, json, datetime

def quote(text): return '\"' + text + '\"'

for line in sys.stdin:
  report = json.loads(line)
  src_ip = report['Client']['IP']
  target_ip = report['Command'][5]
  #local_timestamp = str(datetime.datetime.fromtimestamp(int(report['StartTime'])))
  local_timestamp = int(report['StartTime'])*1000
  # Parse output by newlines, get last line
  last_line = report['Output'].split('\x00')[-2]
  if '???' in last_line: continue # Don't report on an incomplete route

  tokens = last_line.split()
  loss = tokens[2].replace('%', '')
  stddev = tokens[-1]
  avg = tokens[5]

  print([quote(src_ip), quote(target_ip), local_timestamp, loss, avg, stddev])
