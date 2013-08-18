#!/usr/bin/env python 

"""
This script takes a list of devices and adds them to the monitoring (SNMP or live 
checks). All devices on the list that are not already in the database will be 
updated to their new values. Devices that are in the database will but not in the 
new list will be marked as inactive. Devices that are not in the database will
be added using the values provided in the input file.

The Input file is a CSV file that contains the following fields on each line:

<IP>, <NAME>, <ACTIVE>, <DO_SNMP>, <DO_LIVE_CHECK>, <SNMP_COMMUNITY_STRING>
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
import common_functions
import backend

# def update_live_check_list(collection, output_file):
# 	"""
# 	Queries the DB for devices that should be checked
# 	whether they are alive and outputs the list to the
# 	output file
# 	"""
# 	# read a list of devices from the DB which should be included in the live checks
# 	live_check_list = collection.find({'do_live_check': 1, 'status': 1}, {'ip': 1})
# 	(tmp_fd, tmp_file) = tempfile.mkstemp()
# 	live_check_file_handle = os.fdopen(tmp_fd, 'w+')
# 	for elem in live_check_list:
# 		live_check_file_handle.write(str(elem['ip']) + "\n")
# 	live_check_file_handle.close()
# 	# move the tmp file to the final location
# 	os.rename(tmp_file, output_file)

# def update_snmp_poll_list(collection, output_file):
# 	"""
# 	Queries the DB for devices that should be polled 
# 	by SNMP and outputs the list to the output file
# 	along with the snmp community string that is 
# 	needed to perform the query.
# 	"""
# 	# do the same for snmp_pools
# 	# read a list of devices from the DB which should be included in the live checks
# 	snmp_poll_list = collection.find({'do_snmp': 1, 'status': 1}, {'ip': 1, 'community_string': 1})
# 	(tmp_fd, tmp_file) = tempfile.mkstemp()
# 	snmp_poll_file_handle = os.fdopen(tmp_fd, 'w+')
# 	for elem in snmp_poll_list:
# 		snmp_poll_file_handle.write(str(elem['ip']) + " " + str(elem["community_string"]) +  "\n")
# 	snmp_poll_file_handle.close()
# 	# move the tmp file to the final location
# 	os.rename(tmp_file, output_file)
		
if __name__ == "__main__":
	parser = common.get_default_argument_parser("Script for importing a device list into the monitoring process")
	parser.add_argument("device_list_file", help="CSV file that contains a list of devices that should be included into the monitoring process")
	# parser.add_argument("live_check_list", help="Result file that contains a list of IPs that should be checked for whether they are up or not")
	# parser.add_argument("snmp_poll_list", help="Result file that contains a list of IPs and community strings which should be polled by SNMP.")

	args = parser.parse_args()

	# prepare the target database
	dst_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database)

	measurement_map_filename =  os.path.join(os.path.dirname(__file__), "..", "config",  "monitoring_devices.csv")
	for name, fields in common_functions.read_field_dict_from_csv(args.backend, measurement_map_filename).items():
		dst_db.prepareCollection(name, fields)

	device_table = dst_db.getCollection("device_table")

	# now try to read the CSV file
	csv_file_handle = None
	try: 
		csv_file_handle = open(args.device_list_file, 'r')
	except Exception as e:
		print "ERROR: Failed to open input CSV file \"%s\":", e
		sys.exit(-1)

	csv_content = {}
	device_reader = csv.reader(csv_file_handle)
	insert_time = time.time()
	for row in device_reader:
		# expected format:
		#<IP>, <NAME>, <ACTIVE>, <DO_SNMP>, <DO_LIVE_CHECK>, <SNMP_COMMUNITY_STRING>
		if row[0] in csv_content:
			print "ERROR: IP \"" + row[0] + "\" occurs two times in config file. This is not allowed!"
			sys.exit(-2)
		ip = row[0]
		csv_content[ip] = dict()
		csv_content[ip]["name"] = row[1]
		csv_content[ip]["device_type"] = row[2]

		# device status
		if row[3] != "0" and row[3] != "1":
			print "ERROR in CSV: status value of", row[3], "is illegal for IP", ip
			sys.exit(-3)
		csv_content[ip]["status"] = row[3]

		# so_snmp
		if row[4] != "0" and row[4] != "1":
			print "ERROR in CSV: do_snmp value of", row[4], "is illegal for IP", ip
			sys.exit(-3)
		csv_content[ip]["do_snmp"] = row[4]

		# do_live_check
		if row[5] != "0" and row[5] != "1":
			print "ERROR in CSV: do_live_check value of", row[5], "is illegal for IP", ip
			sys.exit(-3)
		csv_content[ip]["do_live_check"] = row[5]

		csv_content[ip]["community_string"] = row[6]

	csv_file_handle.close()


	# get a list of IPs that are already in the DB
	# check which ones of them must be updates, newly created, or marked asinactive
	devices_in_db = device_table.find({})
	for i in devices_in_db:
		db_ip = i['ip']
		if db_ip in csv_content:
			# device already in DB. Check if the status changed
			if str(i["status"]) != csv_content[db_ip]["status"]:
				# status changed. Update timestamp
				print "Status for IP \"" + db_ip + "\" changed in CSV file. Previous: \"" + str( i["status"]) +"\". Now: \"" + str( csv_content[db_ip]["status"]) + "\""
				csv_content[db_ip]["status_changed"] = insert_time
			doc = {}
			print "Updatig entry for IP from values in CSV: ", db_ip
			doc["$set"] = csv_content[db_ip]
			device_table.update({'ip': db_ip}, doc)
			# remove handled ip from csv_reader
			# as we will have a special handling for ips that are 
			# new to the DB
			del csv_content[db_ip]
		else:
			# device is in DB but not in CSV. This means the 
			# device is no longer active. Default policy for such 
			# devices is not to remove them from the DB but to update
			# their status to inactive (if it's not already marked as 
			# active
			if i['status'] != 0:
				print "IP \"" + db_ip + "\" no longer in CSV but marked as active. Changing active state."
				device_table.update({'ip': db_ip}, {"$set": { "status_changed": insert_time, "status": 0}})

	# all the remaining devices can be inserted into the db
	# as they are specified in the CSV input file
	for ip in csv_content:
		print "Found new IP in CSV:", ip
		# set status_changed time to now (as the element is newly 
		# included into the DB and therefore has its first status
		# assigned.
		csv_content[ip]["status_changed"] = insert_time
		doc = {}
		doc["$set"] = csv_content[ip]
		device_table.update({"ip": ip}, doc)
	device_table.flushCache()
	
	# update_live_check_list(device_table, args.live_check_list)
	# update_snmp_poll_list (device_table, args.snmp_poll_list)
