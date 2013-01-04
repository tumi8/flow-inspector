#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Import IPFIX flows from MySQL or PostgreSQL Vermont format
into the Redis buffer for preprocessing

Author: Mario Volke, Lothar Braun 
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
import importer_modules

######### functions


######### main



parser = argparse.ArgumentParser(description="Import IPFIX flows from MySQL or PostgreSQL Vermont format into the Redis buffer for preprocessing")
parser.add_argument("--src-host", nargs="?", default=config.flowDBHost, help="MySQL or PostgreSQL host")
parser.add_argument("--src-port", nargs="?", default=config.flowDBPort, type=int, help="MySQL or PostgreSQL port")
parser.add_argument("--src-user", nargs="?", default=config.flowDBUser, help="MySQL or PostgreSQL user")
parser.add_argument("--src-password", nargs="?", default=config.flowDBPassword, help="MySQL or PostgreSQL password")
parser.add_argument("--src-database", nargs="?", default=config.flowDBName, help="MySQL or PostgreSQL database name")
parser.add_argument("--dst-host", nargs="?", default="127.0.0.1", help="Redis host")
parser.add_argument("--dst-port", nargs="?", default=6379, type=int, help="Redis port")
parser.add_argument("--dst-database", nargs="?", default=0, type=int, help="Redis database")
parser.add_argument("--max-queue", nargs="?", type=int, default=100000, help="The maximum queue length before the import will sleep.")
parser.add_argument("--clear-queue", nargs="?", type=bool, default=False, const=True, help="Whether to clear the queue before importing the flows.")
parser.add_argument("--legacy-vermont", nargs="?", type=bool, default=False, const=True, help="Whether the old legacy VERMONT format should be used")
parser.add_argument("--bro-conn-log", nargs="?", type=bool, default=False, const=True, help="Import files from bro connection logs. If set, --conn-file must be defined.")
parser.add_argument("--argus-db", nargs="?", type=bool, default=False, const=True, help="Import files from argus rasqlinsert")
parser.add_argument("--conn-file", nargs="?", default=None, help="Bro Connection log file. Preprocess will only evaulate this log if --bro-conn-log is set.")
parser.add_argument("--table-name", nargs="?", default=None, help="Table name to import from SQL database.")

args = parser.parse_args()

try:
	r = redis.Redis(host=args.dst_host, port=args.dst_port, db=args.dst_database)
except e:
	print >> sys.stderr, "Could not connect to Redis database: ", e
	sys.exit(1)
	
if args.clear_queue:
	r.delete(common.REDIS_QUEUE_KEY)
	
if args.legacy_vermont:
	print "Importing data from legacy VERMONT db ..."
	importer = importer_modules.get_importer_module("legacy-vermont-db", args)
elif args.bro_conn_log:
	print "Importing data from Bro connection logs ..."
	importer = importer_modules.get_importer_module("bro-importer", args)
elif args.argus_db:
	print "Importing data from Argus MYSQL db ..."
	importer = importer_modules.get_importer_module("argus-importer", args)
else:
	print "Importing data from VERMONT DB ..."
	importer = importer_modules.get_importer_module("vermont-db", args)

startTime = datetime.datetime.now()
print "%s: connected to source and destination database" % (startTime)

print "Starting to import flows ..."
count = 0

while True:
	flow = importer.get_next_flow()
	if flow == None:
		break;

	queue_length = r.rpush(common.REDIS_QUEUE_KEY, json.dumps(flow))
	while queue_length > args.max_queue:
		print "Max queue length reached, importing paused..."
		time.sleep(10)
		queue_length = r.llen(common.REDIS_QUEUE_KEY)
	count += 1
	

common.progress(100, 100)

# Append termination flag to queue
# The preprocessing daemon will terminate with this flag.
r.rpush(common.REDIS_QUEUE_KEY, "END")

endTime = datetime.datetime.now()
print "%s: imported %i flows in %s" % (endTime, count, endTime - startTime)


