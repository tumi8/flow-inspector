#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Import flow data from bro 2.0 (and later) connection logs
into the Redis buffer for preprocessing

Author: Lothar Braun
"""

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import math
import time
import argparse
import datetime
import redis
import json

import common
import config

######### functions
def ip2int(string):
	ip_fields=string.split('.')
	return 2**24*long(ip_fields[0])+2**16*long(ip_fields[1])+2**8*long(ip_fields[2])+long(ip_fields[3])
######### main


parser = argparse.ArgumentParser(description="Import IPFIX flows Bro IDS connection logs into the Redis buffer for preprocessing")
parser.add_argument("--conn-file", nargs="?", default=None, help="Bro Connection log file")
parser.add_argument("--dst-host", nargs="?", default="127.0.0.1", help="Redis host")
parser.add_argument("--dst-port", nargs="?", default=6379, type=int, help="Redis port")
parser.add_argument("--dst-database", nargs="?", default=0, type=int, help="Redis database")
parser.add_argument("--max-queue", nargs="?", type=int, default=100000, help="The maximum queue length before the import will sleep.")
parser.add_argument("--clear-queue", nargs="?", type=bool, default=False, const=True, help="Whether to clear the queue before importing the flows.")

args = parser.parse_args()

if not args.conn_file:
	print "missing argument conn_file!"
	parser.print_help()
	sys.exit(-1)

try:
	input_file = open(args.conn_file, "r")
except Exception as e:
	print >> sys.stderr, "Could not open connection log file ", args.conn_file, ": ", e
	sys.exit(1)

try:
	r = redis.Redis(host=args.dst_host, port=args.dst_port, db=args.dst_database)
except e:
	print >> sys.stderr, "Could not connect to Redis database: ", e
	sys.exit(1)
	
if args.clear_queue:
	r.delete(common.REDIS_QUEUE_KEY)
	
startTime = datetime.datetime.now()
print "%s: connected to destination database and opened source file." % (startTime)

count = 0

for line in input_file:
	# we only support the default format at the moment. 
	# TODO: parse the header, get the appropriate names and 
	# field separators ...
	if line.startswith('#'):
		continue
	
	fields = line.split()	

	srcFlow = {}
	srcFlow[common.COL_FIRST_SWITCHED] = float(fields[0])
	srcFlow[common.COL_SRC_IP] = ip2int(fields[2])
	srcFlow[common.COL_SRC_PORT] = int(fields[3])
	srcFlow[common.COL_DST_IP] = ip2int(fields[4])
	srcFlow[common.COL_DST_PORT] = int(fields[5])
	srcFlow[common.COL_PROTO] = common.getValueFromProto(fields[6])
	if fields[8] == '-':
		srcFlow[common.COL_LAST_SWITCHED] = float(fields[0])
	else:
		srcFlow[common.COL_LAST_SWITCHED] = float(fields[0]) + float(fields[8])
	srcFlow[common.COL_PKTS] = int(fields[15])
	srcFlow[common.COL_BYTES] = int(fields[16])

	dstFlow = {}
	dstFlow[common.COL_FIRST_SWITCHED] = float(fields[0])
	dstFlow[common.COL_SRC_IP] = ip2int(fields[4])
	dstFlow[common.COL_SRC_PORT] = int(fields[5])
	dstFlow[common.COL_DST_IP] = ip2int(fields[2])
	dstFlow[common.COL_DST_PORT] = int(fields[3])
	dstFlow[common.COL_PROTO] = common.getValueFromProto(fields[6])
	if fields[8] == '-':
		dstFlow[common.COL_LAST_SWITCHED] = float(fields[0])
	else:
		dstFlow[common.COL_LAST_SWITCHED] = float(fields[0]) + float(fields[8])
	dstFlow[common.COL_PKTS] = int(fields[17])
	dstFlow[common.COL_BYTES] = int(fields[18])

	for flow in [srcFlow, dstFlow]:
		if flow[common.COL_PKTS] == 0:
			# can happen if only one connection end point was active, e.g. on occurences of scans
			continue
		count += 1
		queue_length = r.rpush(common.REDIS_QUEUE_KEY, json.dumps(flow))
		while queue_length > args.max_queue:
			print "Max queue length reached, importing paused..."
			time.sleep(10)
			queue_length = r.llen(common.REDIS_QUEUE_KEY)


# Append termination flag to queue
# The preprocessing daemon will terminate with this flag.
r.rpush(common.REDIS_QUEUE_KEY, "END")

endTime = datetime.datetime.now()
print "%s: imported %i flows in %s" % (endTime, count, endTime - startTime)
