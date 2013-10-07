#!/usr/bin/env python

"""
This script performs the RTT measurements on the locations
in the database using tools/rtt_measurement.sh. It stores the
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
import common_functions
import snmp_preprocess
import config_snmp_dump


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

	if len(timestamps) != 1:
		print "ERROR: There should only be a single timestamp in the directory. Check your configuration ..."
		sys.exit(1)
	return doc



def perform_rtt_measurement(location_list, rrd_dir, result_collection):
	snmpwalk_pipe = subprocess.Popen([os.path.join(os.path.dirname(__file__), '..', 'tools', 'rtt_measurement.sh'), rrd_dir], stdout=subprocess.PIPE,stdin=subprocess.PIPE)

	input_for_rtt_measurement = ""
	for location in location_list:
		input_for_rtt_measurement += str(location['location_id']) + " " + location['checked_ip'] + '\n'

	output = snmpwalk_pipe.communicate(input=input_for_rtt_measurement)[0].split('\n')
	for entry in output:
		elems = entry.split()
		if len(elems) == 3:
			timestamp = long(elems[0])
			loc_id = elems[1]
			rtt = long(float(elems[2]))
			
			doc = dict()
			entry = dict()
			if rtt == -1:
				entry['success'] = False
				entry['rtt'] = 0
			else:
				entry['success'] = True
				entry['rtt'] = rtt
			doc["$set"] = entry
			result_collection.update({'location_id': loc_id, 'timestamp': timestamp}, doc)
	result_collection.flushCache()

	

if __name__ == "__main__":
	parser = common.get_default_argument_parser("Tool for performing RTT measurements on the monitored locations")

	args = parser.parse_args()

	dst_db = backend.databackend.getBackendObject(
		args.data_backend, args.data_backend_host, args.data_backend_port,
		args.data_backend_user, args.data_backend_password, args.data_backend_database, "INSERT")

	measurement_map_filename =  os.path.join(os.path.dirname(__file__), "..", "config",  "monitoring_devices.csv")
	for name, fields in common_functions.read_field_dict_from_csv(args.data_backend, measurement_map_filename).items():
		dst_db.prepareCollection(name, fields)


	location_table = dst_db.getCollection("location_table")
	results_table  = dst_db.getCollection("location_results")

	# get the ips that should be monitored for snmp availability
	resultSet = location_table.find({}, {'location_id': 1, 'checked_ip': 1});

						    
	perform_rtt_measurement(resultSet, config.rrd_file_dir, results_table)
