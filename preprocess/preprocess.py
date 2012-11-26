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
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import math
import time
import threading
import argparse
import datetime
import redis
import json
import bson
import xml.dom.minidom
from collections import deque

import common
import backend
import config

parser = argparse.ArgumentParser(description="Import IPFIX flows from Redis cache into MongoDB.")
parser.add_argument("--src-host", nargs="?", default="127.0.0.1", help="Redis host")
parser.add_argument("--src-port", nargs="?", default=6379, type=int, help="Redis port")
parser.add_argument("--src-database", nargs="?", default=0, type=int, help="Redis database")
parser.add_argument("--dst-host", nargs="?", default=config.db_host, help="Backend database host")
parser.add_argument("--dst-port", nargs="?", default=config.db_port, type=int, help="Backend database port")
parser.add_argument("--dst-user", nargs="?", default=config.db_user, help="Backend database user")
parser.add_argument("--dst-password", nargs="?", default=config.db_password, help="Backend database password")
parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="Backend database name")
parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole databse before importing any flows.")
parser.add_argument("--backend", nargs="?", default=config.db_backend, const=True, help="Selects the backend type that is used to store the data")

args = parser.parse_args()


# Class to handle flows
class FlowHandler:
	def __init__(self, bucket_interval, collection, nodes_collection, ports_collection, aggr_sum, aggr_values=[], filter_ports=None, cache_size=0):
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
		self.nodes_collection = nodes_collection
		self.ports_collection = ports_collection
		# create indexes
		self.collection.createIndex(common.COL_BUCKET)
		if self.nodes_collection:
			self.nodes_collection.createIndex(common.COL_BUCKET)
			self.nodes_collection.createIndex(common.COL_ID)
		if self.ports_collection:
			self.ports_collection.createIndex(common.COL_BUCKET)
			self.ports_collection.createIndex(common.COL_ID)

		self.aggr_sum = aggr_sum
		self.aggr_values = aggr_values
		self.filter_ports = filter_ports
		
		# init cache
		self.cache = None
		self.cache_size = cache_size
		if cache_size > 0:
			self.cache = dict()
			self.cache_queue = deque()
			
		# stats
		self.num_flows = 0
		self.num_slices = 0
		self.cache_hits = 0
		self.cache_misses = 0
		self.db_requests = 0
		
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
	
	def handleFlow(self, flow):
		"""Slice a flow from the queue into buckets and insert into MongoDB.
		"""
		
		self.num_flows += 1
		
		proto = common.getProto(flow)
		carry = dict();
		emitted = dict();
		for s in self.aggr_sum:
			carry[s] = 0
			emitted[s] = 0
		bucket = self.get_bucket(flow[common.COL_FIRST_SWITCHED], self.bucket_interval)
		while bucket <= flow[common.COL_LAST_SWITCHED]:
		
			self.num_slices += 1
		
			nextBucket = bucket + self.bucket_interval;
			bucketStart = bucket
			if bucketStart < flow[common.COL_FIRST_SWITCHED]:
				bucketStart = flow[common.COL_FIRST_SWITCHED]
			bucketEnd = nextBucket - 1
			if bucketEnd > flow[common.COL_LAST_SWITCHED]:
				bucketEnd = flow[common.COL_LAST_SWITCHED]
			intervalFactor = (bucketEnd - bucketStart + 1) / float(flow[common.COL_LAST_SWITCHED] - flow[common.COL_FIRST_SWITCHED] + 1)
			
			key = self.get_id(bucket, flow)	
			
			# check if we hit the cache
			doc = None
			if self.cache != None:
				doc = self.cache.get(key, None)
				if doc == None:
					self.cache_misses += 1
				else:
					self.cache_hits += 1
			if doc == None:
				doc = { "$set": { common.COL_BUCKET: bucket }, "$inc": {} }
			
				# set unknown ports to None
				if self.filter_ports:
					for v in self.aggr_values:
						if v == common.COL_SRC_PORT or v == common.COL_DST_PORT:
							set_value = None
							value = flow.get(v, None)
							if value != None and value in self.filter_ports:
								filter_proto = int(flow.get(common.COL_PROTO, -1))
								if filter_proto == -1 or filter_proto in self.filter_ports[value]:
									set_value = value
							doc["$set"][v] = set_value
						else:
							doc["$set"][v] = flow.get(v, None)
				else:
					for v in self.aggr_values:
						doc["$set"][v] = flow.get(v, None)
						
				for s in self.aggr_sum:
					doc["$inc"][s] = 0
					doc["$inc"][proto + "." + s] = 0
				doc["$inc"][common.COL_FLOWS] = 0
				doc["$inc"][proto + "." + common.COL_FLOWS ] = 0
				
				if self.cache != None:
					# insert into cache
					self.cache[key] = doc
					self.cache_queue.append(key)
			
			if nextBucket > flow[common.COL_LAST_SWITCHED]:
				for s in self.aggr_sum:
					assert flow.get(s, 0) - emitted[s] >= 0
					doc["$inc"][s] += flow.get(s, 0) - emitted[s]
					# it is possible that the protocol specific part is not yet in the document
					# check and adopt to this
					keyString = proto + "." + s
					if not keyString in doc["$inc"]:
						doc["$inc"][keyString] = flow.get(s, 0) - emitted[s]
					else:
						doc["$inc"][keyString] += flow.get(s, 0) - emitted[s]
			else:
				for s in self.aggr_sum:
					interval = intervalFactor * flow.get(s, 0)
					num = carry[s] + interval
					val = int(num)
					carry[s] = num - val;
					emitted[s] += val
					doc["$inc"][s] += val
					keyString = proto + "." + s
					if not keyString in doc["$inc"]:
						doc["$inc"][keyString] = val
					else:
						doc["$inc"][keyString] += val
					
			# count number of aggregated flows in the bucket
			doc["$inc"][common.COL_FLOWS] += intervalFactor
 
			keyString = proto + "." + common.COL_FLOWS
			if not keyString in doc["$inc"]:
				doc["$inc"][keyString] = intervalFactor
			else:
				doc["$inc"][keyString] += intervalFactor
			
			# if caching is actived then insert into cache
			if self.cache != None:
				self.handleCache()
			else:
				self.updateCollection(key, doc)
				
			bucket = nextBucket
			
	def updateCollection(self, key, doc):
		# bindata will reduce id size by 50%
		#self.collection.update({ "_id": bson.binary.Binary(key) }, doc, True)
		self.collection.update({ "_id": key }, doc, True)
		if self.nodes_collection:
			newdoc = doc["$set"]
			newdoc.update(doc["$inc"])
			common.update_node_index(newdoc, self.nodes_collection, config.flow_aggr_sums)
		if self.ports_collection:
			newdoc = doc["$set"]
			newdoc.update(doc["$inc"])
			common.update_port_index(newdoc, self.ports_collection, config.flow_aggr_sums, known_ports)
		self.db_requests += 1
		
	def handleCache(self, clear=False):
		if not self.cache:
			return
			
		while (clear and len(self.cache_queue) > 0) or len(self.cache_queue) > self.cache_size:
			key = self.cache_queue.popleft()
			doc = self.cache[key]
			self.updateCollection(key, doc)
			del self.cache[key]
	
	def flushCache(self):
		self.handleCache(True)
		self.collection.flushCache()
		if self.nodes_collection:
			self.nodes_collection.flushCache()
		if self.ports_collection:
			self.ports_collection.flushCache()

			
	def printReport(self):
		print "%s report:" % (self.collection.name)
		print "-----------------------------------"
		print "Flows processed: %i" % (self.num_flows)
		if self.num_flows == 0:
			avg_per_flow = 0
		else:
			avg_per_flow = self.num_slices / float(self.num_flows)
		print "Slices overall: %i (avg. %.2f per flow)" % (self.num_slices, avg_per_flow)
		print "Database requests: %i" % (self.db_requests)
		
		if self.cache != None:
			if self.cache_hits == 0:
				hitratio = 0
			else:
				hitratio = self.cache_hits / float(self.cache_hits + self.cache_misses) * 100
			print "Cache hit ratio: %.2f%%" % (hitratio)

		else:
			print "Cache deactivated"
			
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

dst_db = backend.flowbackend.getBackendObject(args.backend, args.dst_host, args.dst_port, args.dst_user, args.dst_password, args.dst_database)
	
if args.clear_database:
	dst_db.clearDatabase()

dst_db.prepareCollections()
	
node_index_collection = dst_db.getCollection(common.DB_INDEX_NODES)
port_index_collection = dst_db.getCollection(common.DB_INDEX_PORTS)
	
known_ports = common.getKnownPorts(config.flow_filter_unknown_ports)

# create flow handlers
handlers = []
for s in config.flow_bucket_sizes:
	handlers.append(FlowHandler(
		s,
		dst_db.getCollection(common.DB_FLOW_PREFIX + str(s)),
		dst_db.getCollection(common.DB_INDEX_NODES + "_" + str(s)),
		dst_db.getCollection(common.DB_INDEX_PORTS + "_" + str(s)),
		config.flow_aggr_sums,
		config.flow_aggr_values,
		known_ports,
		config.pre_cache_size
	))
	
for s in config.flow_bucket_sizes:
	handlers.append(FlowHandler(
		s,
		dst_db.getCollection(common.DB_FLOW_AGGR_PREFIX + str(s)),
		None,
		None,
		config.flow_aggr_sums,
		[],
		None,
		config.pre_cache_size_aggr
	))

print "%s: Preprocessing started." % (datetime.datetime.now())
print "%s: Use Ctrl-C to quit." % (datetime.datetime.now())

timer = threading.Timer(common.OUTPUT_INTERVAL, print_output)
timer.start()

# Daemon loop
while True:
	try:
		# this redis call blocks until there is a new entry in the queue
		obj = r.blpop(common.REDIS_QUEUE_KEY, 0)
		obj = obj[1]
		
		# Terminate if this object is the END flag
		if obj == "END":
			print "%s: Reached END. Terminating..." % (datetime.datetime.now())
			print "%s: Flusing caches. Do not terminate this process or you will have data loss!"
			break
			
		try:
			obj = json.loads(obj)
			obj[common.COL_FIRST_SWITCHED] = int(obj[common.COL_FIRST_SWITCHED])
			obj[common.COL_LAST_SWITCHED] = int(obj[common.COL_LAST_SWITCHED])
			for s in config.flow_aggr_sums:
				obj[s] = int(obj[s])
		except ValueError, e:
			print >> sys.stderr, "Could not decode JSON object in queue: ", e
			continue

		# only import flow if it is newer than config.max_flow_time
		if config.max_flow_age != 0 and obj[common.COL_FIRST_SWITCHED] < (time.mktime(datetime.datetime.utcfromtimestamp(time.time()).timetuple()) - config.max_flow_age):
			print "Flow is too old to be imported into mongodb. Skipping flow ..."
			continue
	
		# Bucket slicing
		for handler in handlers:
			handler.handleFlow(obj)
			
		common.update_node_index(obj, node_index_collection, config.flow_aggr_sums)
		common.update_port_index(obj, port_index_collection, config.flow_aggr_sums, known_ports)
			
		output_flows += 1
		
	except KeyboardInterrupt:
		print "%s: Keyboard interrupt. Terminating..." % (datetime.datetime.now())
		print "%s: Flusing caches. Do not terminate this process or you will have data loss!"
		break
		
timer.cancel()

# clear cache
for handler in handlers:
	handler.flushCache()
# print reports
print ""
for handler in handlers:
	handler.printReport()
