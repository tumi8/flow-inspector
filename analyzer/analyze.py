#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importer
import exporter
import analyzer

import common
import backend
import config

import datetime
import sys

analyzer_store = {}
importer = importer.FlowBackendImporter()
exporter = (exporter.FlowBackendExporter(), exporter.ConsoleExporter())

# Skip first result 
skip = 10
print "Skipping first %s outputs" % skip
for i in range(0, skip):
        (timestamp, data) = importer.getNextDataSet()

	print >> sys.stderr, datetime.datetime.fromtimestamp(int(timestamp))

        for key, initArgs in analyzer.TimeStampAnalyzer.getInstances(data):
            if not key in analyzer_store:
                analyzer_store[key] = analyzer.TimeStampAnalyzer(*initArgs)
            result = analyzer_store[key].passDataSet(data)

# Actual main loop
while True:
	(timestamp, data) = importer.getNextDataSet()

	print >> sys.stderr, datetime.datetime.fromtimestamp(int(timestamp))

	for key, initArgs in analyzer.TimeStampAnalyzer.getInstances(data):
	    if not key in analyzer_store:
	    	analyzer_store[key] = analyzer.TimeStampAnalyzer(*initArgs)
	    result = analyzer_store[key].passDataSet(data)
	    if result != None:
	    	for exp in exporter:
	    		exp.writeEventDataSet(*result)



