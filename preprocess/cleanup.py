#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Automatically remove old data from the mongodb.
Author: Lothar Braun
"""

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import math
import time
import threading
import argparse
import datetime
import redis
import json
import pymongo
import bson
import xml.dom.minidom
from collections import deque

import common
import config

parser = argparse.ArgumentParser(description="Import IPFIX flows from Redis cache into MongoDB.")
parser.add_argument("--host", nargs="?", default=config.db_host, help="MongoDB host")
parser.add_argument("--port", nargs="?", default=config.db_port, type=int, help="MongoDB port")
parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="MongoDB database name")
parser.add_argument("--max_flow_age", nargs="?", default=config.max_flow_age, help="Time in seconds that flows should remain in the mongodb")

args = parser.parse_args()

if args.max_flow_age == 0:
	print "No timeouts for flows defined. No cleanup necessary!"
	sys.exit(0)

print "%s: Init..." % (datetime.datetime.now())

# init pymongo connection
try:
	dst_conn = pymongo.Connection(args.host, args.port)
except pymongo.errors.AutoReconnect, e:
	print >> sys.stderr, "Could not connect to MongoDB database!"
	sys.exit(1)
	

dst_db = dst_conn[args.dst_database]
node_index_collection = dst_db[common.DB_INDEX_NODES]
port_index_collection = dst_db[common.DB_INDEX_PORTS]
	
print "%s: Cleaning started." % (datetime.datetime.now())
print "%s: Use Ctrl-C to quit." % (datetime.datetime.now())

# get all the flow tables
flowTables = [] 
smallestFlowTable = dst_db[common.DB_FLOW_PREFIX + str(min(config.flow_bucket_sizes))]
portIndexTable = dst_db[common.DB_INDEX_NODES]
nodeIndexTable = dst_db[common.DB_INDEX_PORTS]
for s in config.flow_bucket_sizes:
	flowTables.append(dst_db[common.DB_FLOW_PREFIX + str(s)])
	flowTables.append(dst_db[common.DB_FLOW_AGGR_PREFIX + str(s)])

known_ports = common.getKnownPorts(config.flow_filter_unknown_ports)

# Daemon loop
while True:
	try:
		print "Checking for outdated flows ..."
		# get current date in UTC and unix time
		nowUTC = time.mktime(datetime.datetime.utcfromtimestamp(time.time()).timetuple())
		
		# get the timeout value
		timeoutTimeStamp = nowUTC - args.max_flow_age
		print nowUTC, " ",  timeoutTimeStamp

		# get all old flows from the 
		flows = smallestFlowTable.find({"bucket": { "$lt": timeoutTimeStamp}})

		# adapt index to no longer contain those flows
		print "Updating indexes ..."
		for flow in flows:
			common.update_node_index(flow, node_index_collection, config.flow_aggr_sums, common.INDEX_REMOVE)
			common.update_port_index(flow, port_index_collection, config.flow_aggr_sums, known_ports, common.INDEX_REMOVE)

		print "Removing flows ..."
		# remove all old flows from all the aggregates (which means that we simply remove all the buckets ...)
		for table in flowTables:
			table.remove({"bucket" : { "$lt": timeoutTimeStamp}})

		print "Finished cleaning. Waiting for some time ..."
		time.sleep(10)

		
	except KeyboardInterrupt:
		print "%s: Keyboard interrupt. Terminating..." % (datetime.datetime.now())
		break
		

