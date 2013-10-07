#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importer
import exporter
import analyzer

import common
import backend
import config

analyzer_store = {}
importer = importer.FlowBackendImporter()
exporter = (exporter.FlowBackendExporter(), exporter.ConsoleExporter())

while True:
	data = importer.getNextDataSet()

	for key, initArgs in analyzer.IntervalAnalyzer.getInstances(data):
	    if not key in analyzer_store:
	    	analyzer_store[key] = analyzer.IntervalAnalyzer(*initArgs)
	    result = analyzer_store[key].passDataSet(data)
	    if result != None:
	    	for exp in exporter:
	    		exp.writeEventDataSet(*result)

#	for key, initArgs in analyzer.StatusAnalyzer.getInstances(data):
#	    if not key in analyzer_store:
#	    	analyzer_store[key] = analyzer.StatusAnalyzer(*initArgs)
#	    analyzer_store[key].passDataSet(data)


