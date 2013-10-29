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

# import python modules
#import argparse
#import math
#import datetime
#import subprocess

class Importer:

	def __init__(self):
		pass

	def getNextDataSet(self):
		pass

class FlowBackendImporter(Importer):
	
	def __init__(self, last_timestamp = -1):
		# prepare database connection and create required collection objects
		self.db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_table)

		measurement_map_filename =  os.path.join(os.path.dirname(__file__), "..", "config",  "monitoring_devices.csv")
		for name, fields in csv_configurator.read_field_dict_from_csv(config.data_backend, measurement_map_filename).items():
			self.db.prepareCollection(name, fields)

		interface_phy = self.db.getCollection("interface_phy")

		# get all timestamps
		self.timestamps = sorted(interface_phy.distinct("timestamp", {"timestamp": {"$gt": last_timestamp}}))

	def getNextDataSet(self):
		interface_phy = self.db.getCollection("interface_phy")
		timestamp = self.timestamps.pop(0)
		self.last_timestamp = timestamp
		db_result = interface_phy.find({"timestamp": timestamp})
		result = {}
		for data in db_result:
			if not data["router"] in result:
				result[data["router"]] = dict()
			result[data["router"]][data["ifIndex"]] = data
		return (timestamp, result)

	def __getinitargs__(self):
		return (self.last_timestamp,)

	def __getstate__(self):
		return {}


