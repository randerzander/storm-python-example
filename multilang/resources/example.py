#!/usr/bin/python

import storm, sys

class ExampleBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    self.count = 0
    sys.stderr.write('Initializing example python bolt')
  def process(self, tup):
    self.count += 1
    storm.emit(['lol!!!!' + str(self.count)])
    sys.stderr.write(','.join(tup.values) + str(self.count))
    words = tup.values[0].split(' ')
    for word in words: storm.emit([word])


ExampleBolt().run()
