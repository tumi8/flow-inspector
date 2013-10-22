#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importer
import exporter
import analyzer

import common
import backend
import config
import csv_configurator

import cPickle as pickle
import datetime
import sys

if __name__ == "__main__":

	# read analyzer config and store references to analyzer class in analyzerConfig
	analyzerConfig = csv_configurator.readDictionary("AnalyzerConfig.csv")
	analyzerStore = []

	for name, config in analyzerConfig.items():
		importName = __import__(config['class'], fromlist=[config['class']])
		className = importName.__dict__[config['class']]
		analyzerStore.append(className(name, config))

	# assume pickeled state exists
	try:
		importer, exporter, analyzerStore = pickle.load(open('save.analyzer.state', 'rb'))
	
	# else create state
	except:

		importer = importer.FlowBackendImporter()
		exporter = (exporter.FlowBackendExporter(), exporter.ConsoleExporter())
#		exporter = (exporter.FlowBackendExporter(),)
	
		# Skip first results 
		skip = 10
		print "Skipping first %s outputs" % skip
		for i in range(0, skip):
			(timestamp, data) = importer.getNextDataSet()

			print >> sys.stderr, datetime.datetime.fromtimestamp(int(timestamp))
	
			for analyzer in analyzerStore:
				analyzer.passDataSet(data)

	# Actual main loop
	while True:
	
		# Get next data set from Importer
		try:
			(timestamp, data) = importer.getNextDataSet()	
		# No more data available
		except IndexError:
			for exp in exporter:
				exp.flushCache()
#			pickle.dump((importer, exporter, analyzer_store), open('save.analyzer.state', 'wb'))
			sys.exit(0)

		# Print debug information to stderr
		print >> sys.stderr, datetime.datetime.fromtimestamp(int(timestamp))

		# Analyze data

		# Iterate over all active analyzer classes
		for analyzer in analyzerStore:
		
			# pass and process data
			result = analyzer.passDataSet(data)
		
			# export results
			if len(result) > 0:
				for res in result:
					for exp in exporter:
						exp.writeEventDataSet(*res)

		# flush caches and write data to backend
		for exp in exporter:
			exp.flushCache()
		

#		pickle.dump((importer, exporter, analyzer_store), open('save.analyzer.state', 'wb'))
