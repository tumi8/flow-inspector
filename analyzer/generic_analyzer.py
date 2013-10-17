#!/usr/bin/env python 

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))


import csv_configurator
import importer

if __name__ == "__main__":
	analyzer_store = {}

	analyzers = csv_configurator.readDictionary("AnalyzerConfig.csv")
	for analyzer in analyzers:
		importName = __import__(analyzer, fromlist=[analyzer])
		className =  importName.__dict__[analyzer]
		analyzer_object = className()
		print  analyzer_object
#                        moduleConfigName = moduleName + "Config"
#                        if not moduleConfigName in self.config:
#                                print self.config
#                                raise Exception("Could not find section %s for module %s in configuration" % (moduleConfigName, moduleConfigName))


#	importer = importer.FlowBackendImporter()
#	exporter = (exporter.FlowBackendExporter(), exporter.ConsoleExporter())
