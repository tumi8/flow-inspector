#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import sys
import os

from ordered_dict import OrderedDict

class Analyzer:
	""" Generic base class for all analyzers """


	def __init__(self):
		pass

	def passDataSet(self, data):
		pass

	@staticmethod		
	def getInstances(data):
		pass


class IntervalAnalyzer(Analyzer):
	
	def __init__(self, router, interface, field):
		self.router = router
		self.interface = interface
		self.field = field
		self.last_value = 0

		# import read csv functionalities
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'snmp'))
                import common_functions

		values = common_functions.readDictionary("IntervalAnalyzerValues.csv")[field]
		self.st = values['st']
		self.p_st = values['p_st']
		self.ewmv = values['ewmv']
		self.p_ewmv = values['p_ewmv']
		
		self.L = values['L']  

	def passDataSet(self, data):
		router = self.router
		interface = self.interface
		field = self.field
		value = data[router][interface][field]
	 	timestamp = data[router][interface]["timestamp"]
	
		if self.last_value == 0:
			self.last_value = data[router][interface][field]
			return

		t = value - self.last_value
		self.st = self.p_st * t + (1 - self.p_st) * self.st
		self.ewmv = self.p_ewmv * (t - self.st)**2 + (1 - self.p_ewmv) * self.ewmv

		lower_bound = self.st - self.L * math.sqrt(self.ewmv * self.p_st / (2 - self.p_st))
		upper_bound = self.st + self.L * math.sqrt(self.ewmv * self.p_st / (2 - self.p_st))

		parameterdump = OrderedDict([
			("router", self.router),
			("interface", self.interface),
			("last_value", self.last_value),
			("L", self.L),
			("st", self.st),
			("p_st", self.p_st),
			("ewmv", self.ewmv),
			("p_ewmv", self.p_ewmv),
			("value", value),
			("t", t),
			("lower_bound", lower_bound),
			("upper_bound", upper_bound)
		])

		self.last_value = data[router][interface][field]
			
		print >> sys.stderr, parameterdump		

		if lower_bound - t > 6e-14:
			return ("IntervalAnalyzer", router, interface, "LowValue", timestamp, timestamp, "%s < %s" % (t, lower_bound), str(parameterdump))
		if upper_bound - t < -6e-14:
			return ("IntervalAnalyzer", router, interface, "HighValue", timestamp, timestamp, "%s > %s" % (t, upper_bound), str(parameterdump))

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface, "timestamp")) for router in data for interface in data[router])

class StatusAnalyzer(Analyzer):
	
	def __init__(self, router, interface):
		self.router = router
		self.interface = interface

	def passDataSet(self, data):
		router = self.router
		interface = self.interface
		record = data[router][interface]
		if (record["ifOperStatus"] != record["ifAdminStatus"]):
			print "---> Mismatch"
			print time.strftime("%d/%m/%y %H:%M:%S", time.localtime(record["timestamp"])), record["router"], record["ifIndex"], record["ifLastChange"], record["ifAdminStatus"], record["ifOperStatus"]

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface)) for router in data for interface in data[router])
