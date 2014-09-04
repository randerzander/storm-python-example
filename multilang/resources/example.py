#!/usr/bin/python

import storm, sys

class ExampleBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    sys.stderr.write('Initializing example python bolt')
  def process(self, tup):
    sys.stderr.write(','.join(tup.values))
    words = tup.values[0].split(' ')
    for word in words: storm.emit([word])

ExampleBolt().run()
