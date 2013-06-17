#!/usr/bin/env python
# -*- coding: utf-8 -*-

import importer
import analyzer

analyzer_store = {}
importer = importer.FlowBackendImporter()

while True:
	data = importer.getNextDataSet()
	
	for key, initArgs in analyzer.StatusAnalyzer.getInstances(data):
	    if not key in analyzer_store:
	    	analyzer_store[key] = analyzer.StatusAnalyzer(*initArgs)
	    analyzer_store[key].passDataSet(data)


