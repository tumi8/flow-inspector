#!/usr/bin/env python
# -*- coding: utf-8 -*-

import math
import sys

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
	
	def __init__(self, router, interface):
		self.router = router
		self.interface = interface
		self.last_timestamp = 0

		self.st = 0
		self.p_st = 0.25
		self.ewmv = 0
		self.p_ewmv = 0.25
		
		self.L = 1.5  

	def passDataSet(self, data):
		router = self.router
		interface = self.interface
		timestamp = data[router][interface]["timestamp"]
		
		if self.last_timestamp == 0:
			self.last_timestamp = data[router][interface]["timestamp"]
			return

		t = data[router][interface]["timestamp"] - self.last_timestamp
		self.st = self.p_st * t + (1 - self.p_st) * self.st
		self.ewmv = self.p_ewmv * (t - self.st)**2 + (1 - self.p_ewmv) * self.ewmv
		self.last_timestamp = data[router][interface]["timestamp"]

		lower_bound = self.st - self.L * math.sqrt(self.ewmv * self.p_st / (2 - self.p_st))
		upper_bound = self.st + self.L * math.sqrt(self.ewmv * self.p_st / (2 - self.p_st))

		if lower_bound - t > 6e-14:
			return ("IntervalAnalyzer", router, interface, "LowValue", timestamp, timestamp, "%s < %s" % (t, lower_bound))
			# print "%s %s: %s - time too small - %s < %s" % (timestamp, router, interface, t, lower_bound)i
		if upper_bound - t < -6e-14:
			return ("IntervalAnalyzer", router, interface, "HighValue", timestamp, timestamp, "%s > %s" % (t, upper_bound))
			# print "%s %s: %s - time too big - %s > %s" % (timestamp, router, interface, t, upper_bound)

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface)) for router in data for interface in data[router])

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
