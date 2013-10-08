#!/usr/bin/env python 

"""
This script takes a number of locations/network identifier and networks and
puts them into the RTT and loss monitoring. 

The Input file is a CSV file that contains the following fields on each line:

"ID", "DESCRPTION", "CHECKER_IP", "SUBNET_LIST"

where subnet_list is a string that contains subnetstrings in CIDR notation
separated by |
"""

import sys
import os.path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))

import csv 
import time
import tempfile

import common
import csv_configurator
import backend


if __name__ == "__main__":
	parser = common.get_default_argument_parser("Script for importing a device list into the monitoring process")
	parser.add_argument("location_file", help="CSV file that contains a list of locations/networks that should be included into the monitoring process")

	args = parser.parse_args()

	# prepare the target database
	dst_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database)

	measurement_map_filename =  os.path.join(os.path.dirname(__file__), "..", "config",  "monitoring_devices.csv")
	for name, fields in csv_configurator.read_field_dict_from_csv(args.backend, measurement_map_filename).items():
		dst_db.prepareCollection(name, fields)

	location_table = dst_db.getCollection("location_table")

	# now try to read the CSV file
	csv_file_handle = None
	try: 
		csv_file_handle = open(args.location_file, 'r')
	except Exception as e:
		print "ERROR: Failed to open input CSV file \"%s\":", e
		sys.exit(-1)

	csv_content = {}
	location_reader = csv.reader(csv_file_handle)
	insert_time = time.time()
	for row in location_reader:
		# expected format:
		#"ID", "DESCRIPTION", "CHECKER_IP", "SUBNET_LIST"
		if row[0] in csv_content:
			print "ERROR: LocationID \"" + row[0] + "\" occurs two times in config file. This is not allowed!"
			sys.exit(-2)
	        location_id = row[0]
		csv_content[location_id] = dict()
		csv_content[location_id]["description"] = row[1]
		csv_content[location_id]["checked_ip"] = row[2]
		csv_content[location_id]["subnet_list"] = row[3]

	csv_file_handle.close()


	# add all locations to the DB. Update entries if necessary
	for id in csv_content:
		# set status_changed time to now (as the element is newly 
		# included into the DB and therefore has its first status
		# assigned.
		doc = {}
		doc["$set"] = csv_content[id]
		location_table.update({"location_id": id}, doc)
	location_table.flushCache()
