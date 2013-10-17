# -*- coding: utf-8 -*-

import analyzer

class LinkStateAnalyzer(analyzer.Analyzer):
	
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

