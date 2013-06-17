#!/usr/bin/env python
# -*- coding: utf-8 -*-

# import base class
import Importer

# prepare paths
import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..', 'lib'))

# import other modules
import common
import backend
import config

# include python modules
#import argparse
#import math
#import datetime
#import subprocess

class FlowBackendImporter(Importer.Importer):
	
	def __init__(self):
	
		# prepare database connection and create required collection objects
		db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_name)
		interface_phy = db.getCollection("interface_phy")

		# request data from database
		self.data_set = interface_phy.find({}, sort={"timestamp": 1})
		self.last_index = 0
		self.last_timestamp = self.data_set[0]["timestamp"]

	def getNextDataSet(self):
		result = {}
		while (self.last_timestamp == self.data_set[self.last_index]["timestamp"]):
			data = self.data_set[self.last_index]
			if not data["router"] in result:
				result[data["router"]] = dict()
			result[data["router"]][data["ifIndex"]] = data
			self.last_index += 1
		self.last_timestamp = self.data_set[self.last_index]["timestamp"]
		return result
