#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importer
import exporter
import analyzer

import common
import backend
import config
import csv_configurator

import datetime
import sys

if __name__ == "__main__":

	import LinkStateAnalyzer
	import TimestampAnalyzer

	active_analyzers = (LinkStateAnalyzer.LinkStateAnalyzer, TimestampAnalyzer.TimestampAnalyzer)
#	active_analyzers = csv_configurator.readDictionary("AnalyzerConfig.csv")

#	for analyzer in active_analyzers:
#		importName = __import__(analyzer, fromlist=[analyzer])
#		className =  importName.__dict__[analyzer]
#		analyzer_object = className()
#		print  analyzer_object
#			moduleConfigName = moduleName + "Config"
#			if not moduleConfigName in self.config:
#				print self.config
#				raise Exception("Could not find section %s for module %s in configuration" % (moduleConfigName, moduleConfigName))



	analyzer_store = {}
	importer = importer.FlowBackendImporter()
	exporter = (exporter.FlowBackendExporter(), exporter.ConsoleExporter())
#	exporter = (exporter.FlowBackendExporter(),)

	# Skip first result 
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


