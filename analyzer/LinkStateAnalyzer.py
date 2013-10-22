# -*- coding: utf-8 -*-

import analyzer

class LinkStateAnalyzer(analyzer.Analyzer):
	
	def __init__(self, name, parameters):
		self.state = {}
		self.name = name

	def passDataSet(self, data):

		result = []

		for main in data:
			for sub in data[main]:
				try:
					tmp = self.analyzeDataSet(self.state[str(main) + "-" + str(sub)], data)
					if tmp != None:
						result.extend(tmp)
				except:
					self.state[str(main) + "-" + str(sub)] = {
						'router': main,
						'interface': sub,
						'last_ifOperStatus': -1,
						'last_ifAdminStatus': -1,
						'last_timestamp': -1,
						'begin_mismatch': -1
					}
					tmp = self.analyzeDataSet(self.state[str(main) + "-" + str(sub)], data)
					if tmp != None:
						result.extend(tmp)

		return result


	
	def analyzeDataSet(self, state, data):
		
		timestamp = data[state['router']][state['interface']]["timestamp"]
		record = data[state['router']][state['interface']]

		parameterdump = {
			"router": state['router'],
			"interface": state['interface'],
			"last_ifOperStatus": state['last_ifOperStatus'],
			"last_ifAdminStatus": state['last_ifAdminStatus'],
			"last_timestamp": state['last_timestamp'],
			"begin_mismatch": state['begin_mismatch'],
			"timestamp": timestamp,
			"ifOperStatus": record["ifOperStatus"],
			"ifAdminStatus": record["ifAdminStatus"]
		}
		
		result = []

		# check for status mismatch
		if (record["ifOperStatus"] != record["ifAdminStatus"]):
			if state['begin_mismatch'] == -1:
				state['begin_mismatch'] = timestamp
			result.append((self.name, state['router'], state['interface'], "Mismatch", state['begin_mismatch'], timestamp, "%s <> %s" % (record["ifOperStatus"], record["ifAdminStatus"]), str(parameterdump)))
		else:
			state['begin_mismatch'] = -1

		# check for status change in ifOperStatus
		if (record["ifOperStatus"] != state['last_ifOperStatus']):
			result.append((self.name, state['router'], state['interface'], "Change in ifOperStatus", state['last_timestamp'], timestamp, "%s -> %s" % (state['last_ifOperStatus'], record["ifOperStatus"]), str(parameterdump)))
			state['last_ifOperStatus'] = record["ifOperStatus"]

		# check for status change in ifAdminStatus
		if (record["ifAdminStatus"] != state['last_ifAdminStatus']):
			result.append((self.name, state['router'], state['interface'], "Change in ifAdminStatus", state['last_timestamp'], timestamp, "%s -> %s" % (state['last_ifAdminStatus'], record["ifAdminStatus"]), str(parameterdump)))
			state['last_ifAdminStatus'] = record["ifAdminStatus"]

		state['last_timestamp'] = timestamp
		return result

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface)) for router in data for interface in data[router])

