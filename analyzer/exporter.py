#!/usr/bin/env python
# -*- coding: utf-8 -*-

# prepare paths
import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

# import other modules
import common
import backend
import config
import csv_configurator

# include python modules
#import argparse
#import math
#import datetime
#import subprocess

class Exporter:

	def __init__(self):
		pass

	def writeEventDataSet(self, analyzer, mainid, subid, eventtype, start_time, end_time, description, parameterdump):
		pass

	def flushCache(self):
		pass


class ConsoleExporter(Exporter):
	
	def __init__(self):
		self.keys = set()

	def writeEventDataSet(self, analyzer, mainid, subid, eventtype, start_time, end_time, description, parameterdump):
		key = (analyzer, mainid, subid, eventtype, start_time)
	
		from datetime import datetime
		end_time = datetime.fromtimestamp(int(end_time)).strftime("%d.%m.%Y %H:%M:%S")
		
		if key in self.keys:
			print "UPDATE: %s: %s - %s/%s - %s - %s" % (end_time, analyzer, mainid, subid, eventtype, description)
		else:
			print "%s: %s - %s/%s - %s - %s" % (end_time, analyzer, mainid, subid, eventtype, description)
	
		self.keys.add((analyzer, mainid, subid, eventtype, start_time))



class FlowBackendExporter(Exporter):

	def __init__(self):
		# prepare mysql target database
		# TODO: make this more generic later on

		# prepare database connection and create required collection objects
		db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_table, "UPDATE")
		for name, fields in csv_configurator.read_field_dict_from_csv(config.data_backend, "../config/events.csv").items():
			db.prepareCollection(name, fields)
		
		self.events = db.getCollection("events")

	def writeEventDataSet(self, analyzer, mainid, subid, eventtype, start_time, end_time, description, parameterdump):
		self.events.update(
			{"analyzer": analyzer, "mainid": mainid, "subid": subid, "eventtype": eventtype, "start_time": start_time},
			{"$set": {"end_time": end_time, "description": description, "parameterdump": parameterdump}}
		)
		#events.flushCache()
	
	def flushCache(self):
		self.events.flushCache()

	def __getinitargs__(self):
		return ()

	def __getstate__(self):
		return {}
