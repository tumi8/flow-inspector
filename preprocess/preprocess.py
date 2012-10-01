#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Preprocess flows taken from Redis queue.
Keep this script running forever if you want live data:
nohup ./preprocess.py

It is save to run multiple instances of this script!

Author: Mario Volke, Lothar Braun
"""

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))

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
import uuid
from collections import deque

import common
import config

parser = argparse.ArgumentParser(description="Import IPFIX flows from Redis cache into MongoDB.")
parser.add_argument("--src-host", nargs="?", default="127.0.0.1", help="Redis host")
parser.add_argument("--src-port", nargs="?", default=6379, type=int, help="Redis port")
parser.add_argument("--src-database", nargs="?", default=0, type=int, help="Redis database")
parser.add_argument("--dst-host", nargs="?", default=config.db_host, help="MongoDB host")
parser.add_argument("--dst-port", nargs="?", default=config.db_port, type=int, help="MongoDB port")
parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="MongoDB database name")
parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole databse before importing any flows.")

args = parser.parse_args()


# Class to handle flows
class Aggregator:
	def __init__(self, bucket_interval, collection, aggr_sum, aggr_values=[], filter_ports=None):
		"""
		:Parameters:
		 - `bucket_interval`: The bucket interval in seconds.
		 - `collection`: A pymongo collection to insert the documents.
		 - `aggr_sum`: A list of keys which will be sliced and summed up.
		 - `aggr_values`: A list of keys which have to match in order to aggregate two flows
		 - `filter_ports`: A dictionary of ports and protocols to remove unknown ports
		"""
		self.bucket_interval = bucket_interval
		self.collection = collection
		self.aggr_sum = aggr_sum
		self.aggr_values = aggr_values
		self.filter_ports = filter_ports

	def get_id(self, bucket, flow):
		"""Generate a unique id.
       		"""
		id = str(bucket)
		for i,col in enumerate(self.aggr_values):
			id += str(flow.get(col, "x"))
		return id

	def get_bucket(self, timestamp, interval):
		"""Compute the bucket timestamp.
		"""
		return int(timestamp) / int(interval) * int(interval)

	def aggregateFromCollection(self, raw_collection):
		# get flows from the database, create new fields (buckets, and per-protocol fields)
		bucketProject =	{
			'$project': {
				#'bucket': { '$subtract' : [ "$" + common.COL_FIRST_SWITCHED, { "$mod" : [ "$" + common.COL_FIRST_SWITCHED, int(self.bucket_interval) ]} ] }
				common.COL_FIRST_SWITCHED: 1,
				common.COL_LAST_SWITCHED: 1,
				'bucketNums': 
			} 
		}
		
		# include all fields that are either in aggr_sum or aggr_values
		for s in self.aggr_sum:
			bucketProject['$project'][s] = 1
		for v in self.aggr_values:
			bucketProject['$project'][v] = 1
		# make sure that we are always looking at the protocol
		if not common.COL_PROTO in self.aggr_values:
			bucketProject['$project'][common.COL_PROTO] = 1

		for p in common.AVAILABLE_PROTOS:
			bucketProject['$project'][p] = {}
			for s in self.aggr_sum:
				bucketProject['$project'][p][s] = {"$cond": [ { "$eq": [ "$" + common.COL_PROTO, p ] }, "$" + s, 0 ]}

		# take the previously generated data and group it by the aggregation keys
		# ID:
		# generate ID from aggregated values and bucket (TODO: do we need more?)
		aggregateGroup = {
			'$group': {
				'bucket': "$bucket"
			}
		}
		for v in self.aggr_values:
			aggregateGroup['$group'][s] = "$" + v
		for s in self.aggr_values:
			# sum
			aggregateGroup['$group'][v] = { 

		print aggregateGroup
		sys.exit(1)

		pipeline = [ bucketProject ]

#		print pipeline
#		print raw_collection.find()
#		for row in raw_collection.find():
#			print row
#		print raw_collection.find().count()
#		print "Aggregated: ",  raw_collection.aggregate(pipeline)

class FlowHandler:
	def __init__(self, collection, cache_size=1):
		if cache_size <= 0:
			self.cache_size = 1
		else:
			self.cache_size = cache_size

		self.cache_queue = deque()
		self.collection = collection

		# stats
		self.num_flows = 0
			
	def handleFlow(self, flow):
		"""Slice a flow from the queue into buckets and insert into MongoDB.
		"""
		
		self.num_flows += 1
		proto = common.getProto(flow)
		if common.COL_PROTO in flow:
			flow[common.COL_PROTO] = proto
		# bindata will reduce id size by 50%
		self.cache_queue.append(flow)
		if len(self.cache_queue) > self.cache_size:
			self.updateCollection()
			
			
	def updateCollection(self):
		self.collection.insert(self.cache_queue, safe=True)
		self.cache_queue = deque()

	def removeCollection(self):
		self.collection.drop()
		
	def printReport(self):
		print "%s report:" % (self.collection.name)
		print "-----------------------------------"
		print "Flows processed: %i" % (self.num_flows)
		print ""
		
output_flows = 0
def print_output():
	global output_flows, timer
	print "%s: Processed %i flows within last %i seconds (%.2f flows/s)." % (
		datetime.datetime.now(), output_flows, common.OUTPUT_INTERVAL, output_flows / float(common.OUTPUT_INTERVAL))
	output_flows = 0
	timer = threading.Timer(common.OUTPUT_INTERVAL, print_output)
	timer.start()
	
print "%s: Init..." % (datetime.datetime.now())

# init redis connection
try:
	r = redis.Redis(host=args.src_host, port=args.src_port, db=args.src_database)
except Exception, e:
	print >> sys.stderr, "Could not connect to Redis database: %s" % (e)
	sys.exit(1)

# init pymongo connection
try:
	dst_conn = pymongo.Connection(args.dst_host, args.dst_port)
except pymongo.errors.AutoReconnect, e:
	print >> sys.stderr, "Could not connect to MongoDB database!"
	sys.exit(1)
	
if args.clear_database:
	dst_conn.drop_database(args.dst_database)
	
dst_db = dst_conn[args.dst_database]
node_index_collection = dst_db[common.DB_INDEX_NODES]
port_index_collection = dst_db[common.DB_INDEX_PORTS]
	
known_ports = common.getKnownPorts(config.flow_filter_unknown_ports)

# create flow aggregators
flowhandler = FlowHandler(
	dst_db[common.DB_FLOW_RAW + str(uuid.uuid4())],
	config.pre_cache_size
)

aggregators = []
for s in config.flow_bucket_sizes:
	aggregators.append(Aggregator(
		s,
		dst_db[common.DB_FLOW_PREFIX + str(s)],
		config.flow_aggr_sums,
		config.flow_aggr_values,
		known_ports
	))
for s in config.flow_bucket_sizes:
	aggregators.append(Aggregator(
		s,
		dst_db[common.DB_FLOW_AGGR_PREFIX + str(s)],
		config.flow_aggr_sums,
		[],
		None
	))

# create indexes
for aggregator in aggregators:
	aggregator.collection.create_index("bucket")

print "%s: Preprocessing started." % (datetime.datetime.now())
print "%s: Use Ctrl-C to quit." % (datetime.datetime.now())

timer = threading.Timer(common.OUTPUT_INTERVAL, print_output)
timer.start()

lastFirstSwitchedAggregation = 0
# Daemon loop
while True:
	try:
		# this redis call blocks until there is a new entry in the queue
		obj = r.blpop(common.REDIS_QUEUE_KEY, 0)
		obj = obj[1]
		
		# Terminate if this object is the END flag
		if obj == "END":
			print "%s: Reached END. Terminating..." % (datetime.datetime.now())
			break
			
		try:
			obj = json.loads(obj)
			obj[common.COL_FIRST_SWITCHED] = int(obj[common.COL_FIRST_SWITCHED])
			obj[common.COL_LAST_SWITCHED] = int(obj[common.COL_LAST_SWITCHED])
			for s in config.flow_aggr_sums:
				obj[s] = int(obj[s])
		except ValueError, e:
			print >> sys.stderr, "Could not decode JSON object in queue!"
			continue

		# only import flow if it is newer than config.max_flow_time
		if config.max_flow_age != 0 and obj[common.COL_FIRST_SWITCHED] < (time.mktime(datetime.datetime.utcfromtimestamp(time.time()).timetuple()) - config.max_flow_age):
			print "Flow is too old to be imported into mongodb. Skipping flow ..."
			continue
	
		flowhandler.handleFlow(obj)

		if lastFirstSwitchedAggregation == 0:
			lastFirstSwitchedAggregation = obj[common.COL_FIRST_SWITCHED]

		if lastFirstSwitchedAggregation + 600 > obj[common.COL_FIRST_SWITCHED]:
			flowhandler.updateCollection()
			for aggregator in aggregators:
				aggregator.aggregateFromCollection(flowhandler.collection)
			flowhandler.removeCollection()
			flowhandler = FlowHandler(
			        dst_db[common.DB_FLOW_RAW + str(uuid.uuid4())],
			        config.pre_cache_size
				)
			lastFirstSwitchedAggregation = obj[common.COL_FIRST_SWITCHED]
			
		#common.update_node_index(obj, node_index_collection, config.flow_aggr_sums, common.INDEX_ADD)
		#common.update_port_index(obj, port_index_collection, config.flow_aggr_sums, known_ports, common.INDEX_ADD)
			
		output_flows += 1
		
	except KeyboardInterrupt:
		print "%s: Keyboard interrupt. Terminating..." % (datetime.datetime.now())
		break
		
timer.cancel()

# clear cache
#for handler in handlers:
#	handler.handleCache(True)
# print reports
print ""
try:
	print "Processing final aggregation step ..."
	for aggregator in aggregators:
		aggregator.aggregateFromCollection(flowhandler.collection)
except KeyboardInterrupt:
	flowHandler.removeCollection()

print ""
flowhandler.removeCollection()

