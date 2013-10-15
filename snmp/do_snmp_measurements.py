#!/usr/bin/env python

"""
This script performs the SNMP measurements on the systems
in the database using tools/snmpwalk-worker. It stores the
measurement results in RRD files and in the databackend
that is configured in config/config.py
"""

import sys
import os.path
import subprocess
import time
import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

import config
import common
import backend
import csv_configurator
import snmp_preprocess
import config_snmp_dump


def update_interface_status(coll, device_id, if_number, value):
	doc = {}
	entry = {}
	
	if 'ifAlias' in value:
		if_alias = value['ifAlias']
	else:
		if_alias = None

	entry['if_number'] = value['if_number']
	entry['if_alias'] = if_alias
	entry['if_name'] = value['ifName']
	entry['if_status'] = value['ifOperStatus']
	entry['if_type'] = value['ifType']
	doc['$set'] = entry
	coll.update({'device_id': device_id, 'if_number': if_number}, doc)


def update_cpu_status(coll, device_id, cpu):
	doc = {}
	entry = {}
	entry['note'] = ""
	doc['$set'] = entry;
	coll.update({'device_id': device_id, 'cpu_number': cpu}, entry)


def update_mem_status(coll, device_id, mem_number):
	doc = {}
	entry = {}
	entry['note'] = ""
	doc['$set'] = entry
	coll.update({'device_id': device_id, 'pool_number': mem_number}, entry)



def generate_output_string(value, allowed_collections):
	output = ""
	for field in config_snmp_dump.data_source_fields: 
		(collection) = config_snmp_dump.data_source_fields[field]
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
	

def dump_to_rrd(snmp_dump_file, rrd_dir, timestamp):
	os.system(os.path.join(os.path.dirname(__file__), '..', 'tools', 'update-rras') + " " + snmp_dump_file + " " + rrd_dir + " " + str(timestamp))


def prepare_data_and_update_infos(doc, outfile, snmp_ips):
	"""
	This method prepares the incoming data for dumps to RRD database. While doing so
	it updates the info tables in the database for the interfaces, cpu, and memeory counters
	"""
	update_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database, "UPDATE")

	interface_coll = update_db.getCollection("interface_list")
	cpu_list_coll =  update_db.getCollection('cpu_list')
	mem_list_coll =  update_db.getCollection('mem_list')

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

	#print "Dumping interface data to disk ..."

	f_desc = open(outfile, 'w+')
	for key, value in interfaceResultSet.items():
		if not "ifOperStatus" in value or value['ifOperStatus'] != '1':
			continue 
		router = value["router"]
		interface = value["if_number"]
				
		update_interface_status(interface_coll, snmp_ips[router]['_id'], interface, value)

		output = generate_output_string(value, ['interface_phy', 'ifXTable'])
		
		f_desc.write("interface_" + router + "_" + interface + " " + output + "\n")

	# dump cpu sets	
	for key, value in cpuResultSet.items():
		router = value["router"]
		cpu = value["cpu_number"]

		update_cpu_status(snmp_ips[value['router']]['_id'], value['cpu_number'])

		output = generate_output_string(value, [ "ciscoCpu" ])
		f_desc.write("cpu_" + router + "_" + cpu + " " + output + "\n")

	# dump memory sets
	for key, value in memoryResultSet.items():
		router = value["router"]
		pool = value["pool_number"]
		
		update_mem_status(snmp_ips[value['router']]['_id'], value["pool_number"])

		output = generate_output_string(value, [ "ciscoMemory" ])
		f_desc.write("ciscomemory_" + router + "_" + pool + " " + output + "\n")
	f_desc.close()
	interface_coll.flushCache()
	cpu_list_coll.flushCache()
	mem_list_coll.flushCache()


def parse_snmp_data(source_dir):
	"""
	This method reads the content of the snmpwalk-worker files into 
	our python methods. This is necessary for inserting the content 
	into the databackend and into the rrd files. 	     
	"""
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
	
		# output some performance statistics
		time_current = time.time()
		if (time_current - time_last > 5):
			#print "Processed %s lines in %s seconds (%s lines per second)" % (
			#counter, time_current - time_begin, counter / (time_current - time_begin))
			time_last = time_current
	
	if (time_current - time_last > 5):
		#print "Processed %s lines in %s seconds (%s lines per second)" % (
		#counter, time_current - time_begin, counter / (time_current - time_begin))
		time_last = time_current

	if len(timestamps) != 1 and len(timestamps) != 0:
		print "ERROR: There should only be a single timestamp in the directory. Check your configuration ..."
		sys.exit(1)
	return doc


def perform_snmp_measurement(ip_list_community_strings, output_dir):
	"""
	The method checks the availabiliyt of the ip list for snmp
	queries. It takes a list of tuples of (ip_list, community_string)
	that can be used to query the device for the OID ???
	"""
	snmpwalk_pipe = subprocess.Popen([os.path.join(os.path.dirname(__file__), '..', 'tools', 'snmpwalk-worker'), output_dir, config.snmp_oid_file], stdout=subprocess.PIPE,stdin=subprocess.PIPE)

	input_for_snmpwalk_worker = ""
	for ip in ip_list_community_strings:
		community_string = ip_list_community_strings[ip]['community_string']
		input_for_snmpwalk_worker += ip + " " + community_string + "\n"

	output = snmpwalk_pipe.communicate(input=input_for_snmpwalk_worker)[0].split('\n')
	# TODO: decide on what to do with the output. do we want to dump it?
	

if __name__ == "__main__":
	parser = common.get_default_argument_parser("Tool for performing live checks on the devices that require the monitoring")

	args = parser.parse_args()

	dst_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database, "INSERT")

	measurement_map_filename =  os.path.join(os.path.dirname(__file__), "..", "config",  "monitoring_devices.csv")
	for name, fields in csv_configurator.read_field_dict_from_csv(args.backend, measurement_map_filename).items():
		dst_db.prepareCollection(name, fields)


	device_table = dst_db.getCollection("device_table")
	snmp_availability_table = dst_db.getCollection("snmp_availability")

	# get the ips that should be monitored for snmp availability
	resultSet = device_table.find({'do_snmp': 1}, {'_id': 1, 'ip': 1, 'community_string': 1})
	snmp_ips = {}
	for result in resultSet:
		snmp_ips[result['ip']] = result

	# prepare output directories
	# make one directory for each measurmenet run under config.snmp_query_tmp_dir
	# we want to have the directory name as snmp_query_tmp_dir/<current_timestamp>
	# in case we have multiple instances of this script running parallel (e.g.
	# if another instance is started by a cronjob while the old instance is not 
	# yet finished.
	measurement_time = long(time.time())
	output_dir = os.path.join(config.snmp_query_tmp_dir, str(measurement_time))
	if  not os.path.isdir(output_dir):
		os.makedirs(output_dir)

	perform_snmp_measurement(snmp_ips, output_dir)

	doc = parse_snmp_data(output_dir)

	# dump data to rrd files 
	snmp_dump_file = os.path.join(output_dir,"snmp_dump_tmp_file.tmp")

	prepare_data_and_update_infos(doc, snmp_dump_file, snmp_ips)
	if not os.path.isdir(config.rrd_file_dir):
		os.makedirs(config.rrd_file_dir)
	dump_to_rrd(snmp_dump_file, config.rrd_file_dir, measurement_time)

	# dump data to data backend
	collections = snmp_preprocess.prepare_snmp_collections(dst_db, args.backend)
	snmp_preprocess.commit_doc(doc, collections)
	for collection in collections.itervalues():
		collection.flushCache()

	# remove the temporary output directory
	import shutil
	shutil.rmtree(output_dir)
