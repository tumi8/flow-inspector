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

# include python modules
#import argparse
#import math
import datetime
#import subprocess

class Importer:

	def __init__(self):
		pass

	def getNextDataSet(self):
		pass

class FlowBackendImporter(Importer):
	
	def __init__(self):
	
		# prepare database connection and create required collection objects
		self.db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_name)
		interface_phy = self.db.getCollection("interface_phy")

		# get all timestamps
		self.timestamps = sorted(interface_phy.distinct("timestamp"))

	def getNextDataSet(self):
		interface_phy = self.db.getCollection("interface_phy")
		db_result = interface_phy.find({"timestamp": self.timestamps.pop()})
		result = {}
		for data in db_result:
			#if not data["router"] in result:
			result[data["router"]] = dict()
			result[data["router"]][data["ifIndex"]] = data
		print result
		return result
