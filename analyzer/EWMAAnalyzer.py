import analyzer
import csv_configurator
import math
from ordered_dict import OrderedDict

import sys

class EWMAAnalyzer(analyzer.Analyzer):
	
	def __init__(self, parameters):
		
		# store state for individual 'instances'
		self.state = dict()
		
		# initial values for individual instances
		self.st = parameters['st']
		self.ewmv = parameters['ewmv']
	
		# constant parameters for all instances
		self.field = parameters['field']
		self.p_st = parameters['p_st']
		self.p_ewmv = parameters['p_ewmv']
		self.L = parameters['L'] 
		self.tolerance = parameters['tolerance']
		self.differential_mode = parameters['differential_mode']


	# get new data set and pass it to individual instances
	def passDataSet(self, data):

		result = []
	
		# right now data contains only interface_phy
		# so actually main is router, and sub is interface
		for main in data:
			for sub in data[main]:
				try:
					tmp = self.analyzeDataSet(self.state[str(main) + "-" + str(sub)], data)
					if tmp != None:
						result.append(tmp)
				except KeyError:
					self.state[str(main) + "-" + str(sub)] = {
						'mainid': main,
						'subid': sub,
						'last_value': None,
						'st': self.st,
						'ewmv': self.ewmv
					} 
					tmp = self.analyzeDataSet(self.state[str(main) + "-" + str(sub)], data)
					if tmp != None:
						result.append(tmp)
		return result


	# analyze data for one instance
	# state - state for the analyzer, values that differ for each instance, i.e. last_value, st, ewmv
	def analyzeDataSet(self, state, data):

		value = data[state['mainid']][state['subid']][self.field]

		if self.differential_mode:
			if state['last_value'] is None:
				state['last_value'] = data[state['mainid']][state['subid']][self.field]
				return
			t = value - state['last_value']
		else:
			t = value

	 	timestamp = data[state['mainid']][state['subid']]["timestamp"]

		state['st'] = self.p_st * t + (1 - self.p_st) * state['st']
		state['ewmv'] = self.p_ewmv * (t - state['st'])**2 + (1 - self.p_ewmv) * state['ewmv']

		lower_bound = state['st'] - self.L * math.sqrt(state['ewmv'] * self.p_st / (2 - self.p_st))
		upper_bound = state['st'] + self.L * math.sqrt(state['ewmv'] * self.p_st / (2 - self.p_st))

		parameterdump = OrderedDict([
			("mainid", state['mainid']),
			("subid", state['subid']),
			("last_value", state['last_value']),
			("L", self.L),
			("st", state['st']),
			("p_st", self.p_st),
			("ewmv", state['ewmv']),
			("p_ewmv", self.p_ewmv),
			("value", value),
			("t", t),
			("lower_bound", lower_bound),
			("upper_bound", upper_bound)
		])

		state['last_value'] = data[state['mainid']][state['subid']][self.field]
			
		# print >> sys.stderr, parameterdump		

		if lower_bound - t > 6e-14 and lower_bound - t > self.tolerance:
			return (self.__class__.__name__, state['mainid'], state['subid'], "LowValue", timestamp, timestamp, "%s < %s" % (t, lower_bound), str(parameterdump))
		if t - upper_bound > 6e-14 and t - upper_bound > self.tolerance:
			return (self.__class__.__name__, state['mainid'] , state['subid'], "HighValue", timestamp, timestamp, "%s > %s" % (t, upper_bound), str(parameterdump))



