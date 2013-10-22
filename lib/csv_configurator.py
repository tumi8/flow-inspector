import os
import inspect
from ordered_dict import OrderedDict
	
"""
	Our CSV format:
	- each line corresponds to one dictionary entry
	- each level of hierarchy is seperated by ||
	- so the string level 1||level 2||<token> will be converted to
		{
			"level1": {
				"level2": <token>
			}
		}
	- the nesting depth may be arbitrary
	- the <token> encodes the actual variable
	- the type is denoted by the first character
		- 's' encodes string: 'svalue' get string "value"
		- 'i' enocdes integer: 'i42' becomes int(42)
		- 'd' encodes float: 'd42.3' becomes float(42.3)
		- 'f' encodes functions: 'ffunc' gets a reference to function func
			all functions must be importable from the lib directory
		- 't' encode tuples
			- items are seperated by |
			- for each items the same rules as for <token> apply
			- tsstring1|string2|ffunc gets ("string1", "string2", func)
		- 'b' encodes boolean values
		- 'n' encodes NoneType
	- commentary lines begin with #
"""

def readDictionary(file):
	""" read csv file and parse it to dictionary """

	# define local function to parse last token in proper Python types
	def __parseToken(token):
		
		return {
			's': lambda x: x,
			'i': lambda x: int(x),
			'd': lambda x: float(x),
			'f': lambda x: __findFunction(x),
			't': lambda x: tuple(map(__parseToken, x.split("|"))),
			'b': lambda x: bool(x),
			'n': lambda x: None 
		}[token[0]](token[1:])
	
	def __findFunction(string):
		# does not allow for nested functions right now
		# split name into parts	
		string = string.split(".")
		# import module
		module = __import__(string[0], globals(), locals(), [], -1)
		# return function object
		return module.__dict__[string[1]]
	
	# recursively merge dictionary right into dictionary left
	def __recursiveUpdate(left, right):
		for key, value in right.iteritems():
			if isinstance(value, dict):
				tmp = __recursiveUpdate(left.get(key, OrderedDict()), value)
				left[key] = tmp
			else:
				left[key] = right[key]
		return left

	# create dictionary for output
	out_dict = OrderedDict()

	# open file and iterate over all lines expects empty ones
	f = open(file, 'r')
	for line in f:
		if (line != "\n") and (not line.startswith("#")):

			# split line into segments
			items = line.strip().split("||")

			# parse last item
			tmp_dict = __parseToken(items[-1])

			# create hierachy
			for item in items[:-1:][::-1]:
				tmp_dict = {item: tmp_dict}
			
			# merge temporaly built dictionary into output dictionary
			__recursiveUpdate(out_dict, tmp_dict)
	return out_dict

def dict2csv(dict, prefix=""):
	""" convert dictionary to our csv format """

	# local function for converting variables to our csv format
	def __parseVariable(var):

		# parse tuples
		if type(var) == types.TupleType:
			
			# recurse into __parseVariable because each item of a tuple must be converted
			tuplestring = "t" + __parseVariable(var[0])
			for item in var[1:]:
				tuplestring += "|" + __parseVariable(item)
			return tuplestring

		# parse functions
		elif type(var) == types.FunctionType:
			return "f" + var.__name__
		
		# parse None
		elif var == None:
			return "n"

		# else assume we're dealing with a string
		else:
			return "s" + var

	# recursively iterate through the dictionary
	for key, value in dict.iteritems():
		if prefix == "":
				tmp_prefix = key + "||"
		else:
			tmp_prefix = prefix + key + "||"

		if type(value) == type(dict):
			dict2csv(value, tmp_prefix)
		else:
			print tmp_prefix + __parseVariable(value)



def create_fieldDict(backend, convert_generic, convert_snmp, csv_file):
	field_map = readDictionary(os.path.join(os.path.dirname(__file__), '..', 'config', csv_file))
	fieldDict = OrderedDict() 

	for key, dictionary in field_map.iteritems():
		name = dictionary['name']
		table = dictionary['table']
		if (not 'only' in dictionary) or (dictionary['only'] == backend):
			if not table in fieldDict:
				fieldDict[table] = OrderedDict()
	
			if 'use_type' in dictionary:
				if dictionary['use_type'] == 'predef_value':
					fieldDict[table][name] = dictionary['value']
				else:
					fieldDict[table][name] = convert_generic(dictionary['use_type'])
			else:
				fieldDict[table][name] = convert_snmp(dictionary['snmp_type'])
	return fieldDict

def read_field_dict_from_csv(backend, csv_file):
	if backend == "mysql":
		type_snmp_generic = readDictionary(os.path.join(os.path.dirname(__file__), '..', 'config', "snmp_generic.csv"))
		type_generic_mysql = readDictionary(os.path.join(os.path.dirname(__file__), '..', 'config', "generic_mysql.csv"))
		
		def __convert_generic(generic_type):
			return type_generic_mysql[generic_type]
		
		def __convert_snmp(snmp_type):
			return type_generic_mysql[type_snmp_generic[snmp_type]]
		
		return create_fieldDict('mysql', __convert_generic, __convert_snmp, csv_file)
	elif backend == "oracle":
		type_snmp_generic = readDictionary(os.path.join(os.path.dirname(__file__), '..', 'config', "snmp_generic.csv"))
		type_generic_oracle = readDictionary(os.path.join(os.path.dirname(__file__), '..', 'config', "generic_oracle.csv"))
		
		def __convert_generic(generic_type):
			return type_generic_oracle[generic_type]
		
		def __convert_snmp(snmp_type):
			return type_generic_oracle[type_snmp_generic[snmp_type]]
		
		return create_fieldDict('oracle', __convert_generic, __convert_snmp, csv_file)
	elif args.backend == "mongo":
		return None
	else:
		raise Exception("Unknown data backend: " + args.backend);
