#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importer
import exporter
import analyzer

import common
import backend
import config
import csv_configurator

import pickle
import datetime
import sys

if __name__ == "__main__":

	# read analyzer config and store references to analyzer class in active_analyyers
	active_analyzers = []
	analyzers = csv_configurator.readDictionary("AnalyzerConfig.csv")
	for analyzer in analyzers:
		importName = __import__(analyzer, fromlist=[analyzer])
		className =  importName.__dict__[analyzer]
		active_analyzers.append(className)
#		analyzer_object = className()
#		print  analyzer_object
#		moduleConfigName = className.__name__ + "Config"
#		if not moduleConfigName in config:
#			print config
#			raise Exception("Could not find section %s for module %s in configuration" % (moduleConfigName, moduleConfigName))


	# assume pickeled state exists
	try:
		importer, exporter, analyzer_store = pickle.load(open('save.analyzer.state', 'rb'))
	
	# create state
	except:

		analyzer_store = {}
		importer = importer.FlowBackendImporter()
		exporter = (exporter.FlowBackendExporter(), exporter.ConsoleExporter())
#		exporter = (exporter.FlowBackendExporter(),)
	
		# Skip first results 
		skip = 10
		print "Skipping first %s outputs" % skip
		for i in range(0, skip):
			(timestamp, data) = importer.getNextDataSet()

			print >> sys.stderr, datetime.datetime.fromtimestamp(int(timestamp))
	
			for analyzer_class in active_analyzers:
				for key, initArgs in analyzer_class.getInstances(data):
					# make keys unique
					key = analyzer_class.__name__ + key
					if not key in analyzer_store:
						analyzer_store[key] = analyzer_class(*initArgs)
					analyzer_store[key].passDataSet(data)

	# Actual main loop
	while True:
	
		for exp in exporter:
			exp.flushCache()

		(timestamp, data) = importer.getNextDataSet()

		print >> sys.stderr, datetime.datetime.fromtimestamp(int(timestamp))

		for analyzer_class in active_analyzers:
			for key, initArgs in analyzer_class.getInstances(data):
				# make keys unique
				key = analyzer_class.__name__ + key
				if not key in analyzer_store:
					analyzer_store[key] = analyzer_class(*initArgs)

				result = analyzer_store[key].passDataSet(data)

				if result != None:
					try:
						for res in result:
							for exp in exporter:
								exp.writeEventDataSet(*res)
					except:	
						for exp in exporter:
							exp.writeEventDataSet(*result)

		pickle.dump((importer, exporter, analyzer_store), open('save.analyzer.state', 'wb'))
