import os
from ordered_dict import OrderedDict

# parsing function for oid values
def plain(value):
	""" do nothing function """
	return value


def hex2ip(value):
	""" convert hex to ip (i.e. DE AD BE EF -> 222.173.190.239) """
	value = value.strip(" ")
	return '.'.join(str(int(part, 16))
		for part in [value[0:2], value[3:5], value[6:8], value[9:11]])


def netmask2int(netmask):
	""" convert netmask to int (i.e. 255.255.255.0 -> 24) """
	tmp = ''
	for part in netmask.split("."):
		tmp = tmp + str(bin(int(part)))
	return tmp.count("1")


def int2netmask(value):
	""" convert int to netmask (i.e. 24 -> 255.255.255.0) """
	value = '1' * int(value) + '0' * (32 - int(value))
	return '.'.join(str(int(part, 2))
		for part in [value[0:8], value[8:16], value[16:24], value[24:32]])


def ip2int(ip):
	""" convert ip to int """
	ip = ip.split('.')
	return (int(ip[0]) * (2 ** 24) + int(ip[1]) * (2 ** 16) +
		int(ip[2]) * (2 ** 8) + int(ip[3]))


def int2ip(i):
	""" convert int to ip """
	return (str(i // (2 ** 24)) + "." + str((i // (2 ** 16)) % 256) + "." +
		str((i // (2 ** 8)) % 256) + "." + str(i % 256))

def hex2ip2int(value):
	return ip2int(hex2ip(value))

def calc_ip_range(ip, mask):
	""" calculate smallest and biggest ip belonging to given network """
	
	mask_inv = 32 - int(mask)

	# strip network bits
	bits = int(ip) >> mask_inv

	# calculate network address by shifting network bits to left
	# efficitvely zeros the '(32-mask)'-right bits of ip
	low_ip = bits << mask_inv

	# calculate broadcast address by filling 'mask'-right bits with 1
	high_ip = low_ip + (2**mask_inv - 1)

	return (low_ip, high_ip)


	
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
			- 't' encode tuples
				- items are seperated by |
				- for each items the same rules as for <token> apply
				- tsstring1|string2|ffunc gets ("string1", "string2", func)
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
			'f': lambda x: globals()[x],
			't': lambda x: tuple(map(__parseToken, x.split("|"))),
			'n': lambda x: None 
		}[token[0]](token[1:])
	
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
			return type_generic_mysql[generic_type]
		
		def __convert_snmp(snmp_type):
			return type_generic_oracle[type_snmp_generic[snmp_type]]
		
		return create_fieldDict('oracle', __convert_generic, __convert_snmp, csv_file)
	elif args.backend == "mongo":
		return None
	else:
		raise Exception("Unknown data backend: " + args.backend);
