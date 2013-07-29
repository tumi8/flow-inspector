#!/usr/bin/env python

'''
This script configures the graphs and stuff that are needed by the
external web app (not included in this source code tree yet). 

@author: braun
'''

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

def create_interface_list(device_coll, interface_phy_coll, ifxtable_coll, destination_coll):
	"""
	Extract all devices that are known to ifXTable and interface_phy.
	Merge their values (e.g. status, if_names) into a consistent view
	and store this view in the database for quick access by the web
	frontend application.
	"""
	print "Fetching list of devices and interfaces from ifxTable ..."
	interface_results = ifxtable_coll.find({}, fields={"DISTINCT router" : 1, "if_number" : 1, 'ifName': 1, 'ifAlias': 1})

	print "Fetching list of interface statuses ..."
	iface_phy_results = interface_phy_coll.find({}, fields={"DISTINCT router" : 1, "if_number" : 1, "ifOperStatus": 1, "ifType":1})

	print "Retrieving a list of device ids ..."
	deviceresults = device_table.find({}, fields={"IP": 1, "_id": 1})
	devices_map = dict()
	for result in deviceresults:
		print result

	interface_list = {}
	print "Building interface list ..."
	for r in interface_results:
		ip = r['router']
		if_num = r["if_number"] 
		if if_num == None:
			print "Found interface without if_number:", r
			continue
		else:
			if_num = str(if_num)
		interface_list[ip + "_" + if_num] = r
	

	

def create_graph_templates(collection):
	"""
	Insert graph configs from config_snmp_dump into the collection
	"""
	for graph in config_snmp_dump.graph_dict:
		graph_config = config_snmp_dump.graph_dict[graph]
		for item in graph_config:
			(field1, field2, title, vert_label, scale) = item
			doc = dict()
			table_entry = dict()
			table_entry["type"] = graph
			table_entry["field1"] = field1
			table_entry["field2"] = field2
			table_entry["vert_legend"] = vert_label
			table_entry["scale"] = scale
			table_entry["start_time"] = -86400
			table_entry["end_time"] = -300
			doc["$set"] = table_entry
			collection.update({"graph_name": title}, doc)
	collection.flushCache()


if __name__ == "__main__":
	parser = common.get_default_argument_parser("Tool for performing live checks on the devices that require the monitoring")

	args = parser.parse_args()

	dst_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database)

	measurement_map_filename =  os.path.join(os.path.dirname(__file__), "..", "config",  "monitoring_devices.csv")
	for name, fields in common_functions.read_field_dict_from_csv(args.backend, measurement_map_filename).items():
		dst_db.prepareCollection(name, fields)

	create_graph_templates(dst_db.getCollection("graph_list"))
	create_interface_list(dst_db.getCollection("interfaces"))
	
