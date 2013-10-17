# -*- coding: utf-8 -*-

import math
import sys
import os

import csv_configurator
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


class EWMAAnalyzer(Analyzer):
	
	def __init__(self, mainid, subid, field):
		self.mainid = mainid 
		self.subid = subid
		self.field = field
		self.last_value = None

		values = csv_configurator.readDictionary("AnalyzerConfig.csv")[self.__class__.__name__]
		self.st = values['st']
		self.p_st = values['p_st']
		self.ewmv = values['ewmv']
		self.p_ewmv = values['p_ewmv']
		self.L = values['L'] 
		self.tolerance = values['tolerance']
	
	def passDataSet(self, data):
		mainid = self.mainid
		subid = self.subid
		field = self.field
		value = data[mainid][subid][field]
	 	timestamp = data[mainid][subid]["timestamp"]
	
		if self.last_value is None:
			self.last_value = data[mainid][subid][field]
			return

		t = value - self.last_value
		self.st = self.p_st * t + (1 - self.p_st) * self.st
		self.ewmv = self.p_ewmv * (t - self.st)**2 + (1 - self.p_ewmv) * self.ewmv

		lower_bound = self.st - self.L * math.sqrt(self.ewmv * self.p_st / (2 - self.p_st))
		upper_bound = self.st + self.L * math.sqrt(self.ewmv * self.p_st / (2 - self.p_st))

		parameterdump = OrderedDict([
			("mainid", self.mainid),
			("subid", self.subid),
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

		self.last_value = data[mainid][subid][field]
			
		# print >> sys.stderr, parameterdump		

		if lower_bound - t > 6e-14 and lower_bound - t > self.tolerance:
			return (self.__class__.__name__, mainid, subid, "LowValue", timestamp, timestamp, "%s < %s" % (t, lower_bound), str(parameterdump))
		if t - upper_bound > 6e-14 and t - upper_bound > self.tolerance:
			return (self.__class__.__name__, mainid, subid, "HighValue", timestamp, timestamp, "%s > %s" % (t, upper_bound), str(parameterdump))



class TimeStampAnalyzer(EWMAAnalyzer):

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface, "timestamp")) for router in data for interface in data[router])



class LinkStateAnalyzer(Analyzer):
	
	def __init__(self, router, interface):
		self.router = router
		self.interface = interface
		self.last_ifOperStatus = -1
		self.last_ifAdminStatus = -1
		self.last_timestamp = -1
		self.begin_mismatch = -1

	def passDataSet(self, data):
		timestamp = data[self.router][self.interface]["timestamp"]

		record = data[self.router][self.interface]

		parameterdump = {
			"router": self.router,
			"interface": self.interface,
			"last_ifOperStatus": self.last_ifOperStatus,
			"last_ifAdminStatus": self.last_ifAdminStatus,
			"last_timestamp": self.last_timestamp,
			"begin_mismatch": self.begin_mismatch,
			"timestamp": timestamp,
			"ifOperStatus": record["ifOperStatus"],
			"ifAdminStatus": record["ifAdminStatus"]
		}

		
		result = set()

		# check for status mismatch
		if (record["ifOperStatus"] != record["ifAdminStatus"]):
			if self.begin_mismatch == -1:
				self.begin_mismatch = timestamp
			result.add((self.__class__.__name__, self.router, self.interface, "Mismatch", self.begin_mismatch, timestamp, "%s <> %s" % (record["ifOperStatus"], record["ifAdminStatus"]), str(parameterdump)))
		else:
			self.begin_mismatch = -1

		# check for status change in ifOperStatus
		if (record["ifOperStatus"] != self.last_ifOperStatus):
			result.add((self.__class__.__name__, self.router, self.interface, "Change in ifOperStatus", self.last_timestamp, timestamp, "%s -> %s" % (self.last_ifOperStatus, record["ifOperStatus"]), str(parameterdump)))
			self.last_ifOperStatus = record["ifOperStatus"]

		# check for status change in ifAdminStatus
		if (record["ifAdminStatus"] != self.last_ifAdminStatus):
			result.add((self.__class__.__name__, self.router, self.interface, "Change in ifAdminStatus", self.last_timestamp, timestamp, "%s -> %s" % (self.last_ifAdminStatus, record["ifAdminStatus"]), str(parameterdump)))
			self.last_ifAdminStatus = record["ifAdminStatus"]

		self.last_timestamp = timestamp
		return result

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface)) for router in data for interface in data[router])


