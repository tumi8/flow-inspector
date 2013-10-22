import analyzer
import csv_configurator
import math

from ordered_dict import OrderedDict

class ThresholdAnalyzer(analyzer.Analyzer):
	
	def __init__(self, parameters):
		
		# store state for individual 'instances'
		self.state = dict()
		
	
		# constant parameters for all instances
		self.field = parameters['field']
		self.limit = parameters['limit']
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
				except:
					self.state[str(main) + "-" + str(sub)] = {
						'mainid': main,
						'subid': sub,
						'last_value': None
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
			value = value - state['last_value']

		timestamp = data[state['mainid']][state['subid']]["timestamp"]

		parameterdump = OrderedDict([
			("mainid", state['mainid']),
			("subid", state['subid']),
			("limit", self.limit),
			("field", self.field),
			("value", value)
		])

		if value > self.limit:
			return (self.__class__.__name__, state['mainid'], state['subid'], "ValueException", timestamp, timestamp, "%s > %s" % (value, self.limit), str(parameterdump))

