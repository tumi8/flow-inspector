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
import csv_configurator
import snmp_preprocess
import config_snmp_dump
		

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
	for name, fields in csv_configurator.read_field_dict_from_csv(args.backend, measurement_map_filename).items():
		dst_db.prepareCollection(name, fields)
	snmp_preprocess.prepare_snmp_collections(dst_db, args.backend)

	create_graph_templates(dst_db.getCollection("graph_list"))

	device_coll = dst_db.getCollection("device_table")
	interface_phy_coll = dst_db.getCollection("interface_phy")
	ifxtable_coll = dst_db.getCollection("ifXTable")
	destination_coll = dst_db.getCollection("interface_list")
	cpu_list = dst_db.getCollection('ciscoCpu')
	mem_list = dst_db.getCollection('ciscoMemory')

	create_interface_list(device_coll, interface_phy_coll, ifxtable_coll, destination_coll)
	create_cpu_list(cpu_list)
	create_mem_list(mem_list)
	
	
