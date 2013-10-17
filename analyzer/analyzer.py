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
