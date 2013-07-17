#!/usr/bin/python
'''

Created on 20.03.2013

@author: braun
'''

import config_snmp_dump

import argparse
import os, sys
import snmp_preprocess
import time
import glob

import backend
import config

############################### some globals 

def generate_output_string(value, allowed_collections):
	output = ""
	for field in config_cacti.data_source_fields: 
		(collection) = config_cacti.data_source_fields[field]
		if not collection in allowed_collections:
			continue
		# TODO: We assume that the ordering is good. There should be some checks for that ...
		try:
			v = value[field]
		except:
			v = None
		if v != None:
			fieldValue = long(v)
		else: 
			fieldValue = 0
		if output != "":
			output += " "
		output += field + ":" + str(fieldValue)
	return output
	
def parse_snmp_data(source_dir):
	doc = {}
	timestamps = set()
	files = glob.glob(source_dir + "/*.txt")
	time_begin = time.time()
	time_current = time.time()
	time_last = time.time()
	counter = 0
	for file in files:
		(read_lines, timestamp, doc) = snmp_preprocess.parse_snmp_file(file, doc)
		counter += read_lines
		timestamps.add(timestamp)
	
		# do statistical calculation
		time_current = time.time()
		if (time_current - time_last > 5):
			print "Processed %s lines in %s seconds (%s lines per second)" % (
			counter, time_current - time_begin, counter / (time_current - time_begin))
			time_last = time_current
	
	if (time_current - time_last > 5):
		print "Processed %s lines in %s seconds (%s lines per second)" % (
		counter, time_current - time_begin, counter / (time_current - time_begin))
		time_last = time_current

	if len(timestamps) != 1:
		print "ERROR: There should only be a single timestamp in the directory. Check your configuration ..."
		sys.exit(1)
	return doc


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Preprocess SNMP data")
	parser.add_argument("source_directory", nargs=1, help="Source directory which contains snmpbulkwalk files")
	parser.add_argument("target_directory", nargs=1, help="Destination directory which contains cache files for cacti/update-rras")
	parser.add_argument("--dst-host", nargs="?", default=config.data_backend_host,
				help="Backend database host")
	parser.add_argument("--dst-port", nargs="?", default=config.data_backend_port,
				type=int, help="Backend database port")
	parser.add_argument("--dst-user", nargs="?", default=config.data_backend_user,
				help="Backend database user")
	parser.add_argument("--dst-password", nargs="?",
				default=config.data_backend_password, help="Backend database password")
	parser.add_argument("--dst-database", nargs="?",
				default=config.data_backend_snmp_name, help="Backend database name")
	parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True,
				help="Whether to clear the whole databse before importing any flows.")
	parser.add_argument("--backend", nargs="?", default=config.data_backend, const=True,
				help="Selects the backend type that is used to store the data")

	args = parser.parse_args()
	target_dir = args.target_directory[0]
	source_dir = args.source_directory[0]

############################### start of processing

	print "Parsing snmp data ..."
	doc = parse_snmp_data(source_dir)
	print "Merging interface data ..."
	
	interfaceResultSet = {}
	cpuResultSet = {}
	memoryResultSet = {}
	for name,table in doc.items():
		if name == "ifXTable" or name == "interface_phy":
			for value in table.itervalues():
				keys = value[0]
				dict_key = keys['router'] + "-" + keys['if_number']
				if dict_key in interfaceResultSet:
					a = interfaceResultSet[dict_key]
					b = value[1]["$set"]
					interfaceResultSet[dict_key] = dict(a.items() + b.items())
				else:
					interfaceResultSet[dict_key] = dict(keys.items() + value[1]["$set"].items())
		if name == "ciscoCpu":
			for value in table.itervalues():
				keys = value[0]
				dict_key = keys['router'] + '-' + keys["cpu_number"];
				if dict_key in cpuResultSet:
					# this should never happen. Log an error
					print "Error: Device", keys['router'], "with CPU", keys["cpu_number"], "is already in result dictionary. Please investigate."
				cpuResultSet[dict_key] = dict(keys.items() + value[1]["$set"].items())
		if name == "ciscoMemory":
			for value in table.itervalues():
				keys = value[0]
				dict_key = keys['router'] + '-' + keys["pool_number"];
				if dict_key in memoryResultSet:
					# this should never happen. Log an error
					print "Error: Device", keys['router'], "with Pool", keys["pool_number"], "is already in result dictionary. Please investigate."
				memoryResultSet[dict_key] = dict(keys.items() + value[1]["$set"].items())
			

	print "Dumping interface data to disk ..."

	outfile = os.path.join(target_dir, "snmp_info_dumps.txt")
	f_desc = open(outfile, 'w+')
	for key, value in interfaceResultSet.items():
		if not "ifOperStatus" in value or value['ifOperStatus'] != '1':
			continue 
		router = value["router"]
		interface = value["if_number"]

		output = generate_output_string(value, ['interface_phy', 'ifXTable'])
		
		f_desc.write("interface_" + router + "_" + interface + " " + output + "\n")

	# dump cpu sets	
	for key, value in cpuResultSet.items():
		router = value["router"]
		cpu = value["cpu_number"]

		output = generate_output_string(value, [ "ciscoCpu" ])
		f_desc.write("cpu_" + router + "_" + cpu + " " + output + "\n")

	# dump memory sets
	for key, value in memoryResultSet.items():
		router = value["router"]
		pool = value["pool_number"]

		output = generate_output_string(value, [ "ciscoMemory" ])
		f_desc.write("ciscomemory_" + router + "_" + pool + " " + output + "\n")


	f_desc.close()
	
