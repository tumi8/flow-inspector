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

######### functions

def getTableNameFromTimestamp(timestamp):
	timeObj = datetime.datetime.utcfromtimestamp(timestamp)
	if timeObj.minute < 30:
		halfHour = 0
	else:
		halfHour = 1
	tableName = "h_%0.4d%0.2d%0.2d_%0.2d_%0.1d" % (timeObj.year, timeObj.month, timeObj.day, timeObj.hour, halfHour)
	return tableName

def getTables(tables, firstTimestamp, lastTimestamp, TYPE):
	"""
		Expects a sorted list of table names (sorted by time).
		Returns the table which contains the first flow that 
		has a firstSwitched time of firstTimestamp
	"""
	if firstTimestamp == 0 and lastTimestamp == 0:
		return tables
	
	# restrict tablespace on first timestamp
	if firstTimestamp != 0:
		firstTableName = getTableNameFromTimestamp(firstTimestamp)

		try:
			idx = tables.index(firstTableName) 
		except:
			# no such table in list
			return []
		tables =  tables[idx:len(tables)]
	if lastTimestamp != 0:
		lastTable = getTableNameFromTimestamp(lastTimestamp)
		try:
			idx = tables.index(lastTable)
		except:
			# no such table in list
			return tables
		tables = tables[0:idx]
	return tables
 

# width defines bar width
# percent defines current percentage
def progress(width, percent):
	marks = math.floor(width * (percent / 100.0))
	spaces = math.floor(width - marks)
 
 	loader = '[' + ('=' * int(marks)) + (' ' * int(spaces)) + ']'
 
	sys.stdout.write("%s %d%%\r" % (loader, percent))
	if percent >= 100:
		sys.stdout.write("\n")
	sys.stdout.flush()

def compareTables(a, b):
	compsA = a.split('_')
	compsB = b.split('_')
	return -cmp(int(compsA[1]), int(compsB[1])) or cmp(int(compsA[2]), int(compsB[2])) or cmp(int(compsA[3]), int(compsB[3]))



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
parser.add_argument("--continuous-update", nargs="?", type=bool, default=False, const=True, help="Whether the database import should stop after it reaches the end of the database, or whether it should continue to get new flows")
parser.add_argument("--start-time", nargs="?", type=int,  default=0, const=True, help="Defines the offset in unix time at which flow importing into mongo should start. This is handy for large flow-database where we do not want to import the complete database. Default: 0 (import all tables)")
parser.add_argument("--end-time", nargs="?", type=int, default=0, const=True, help="Defines the offset in unix time at which flow importing into mongo should stop. This is handy  for large flow-databases where we do not want to import the complete database. Default: 0 (import all tables)")
parser.add_argument("--from-temporary", nargs="?", type=bool, default=False, const=True, help="Import flows that have been generated with app/pcapprocess/check-pcap.py")

args = parser.parse_args()


if args.from_temporary:
	try:
		import pymongo
		conn = pymongo.Connection(args.src_host, args.src_port)
		srcDB = conn['pcap']
		connections = srcDB['all_flows']
		TYPE = "mongodb"
	except Exception as inst:
		print "Could not connect to mongo db: ", inst
		sys.exit(-1)
else:
	# check if is there a MySQL or a PostgreSQL database
	try:
		print "Trying postgresql database ..."
		import psycopg2
		TYPE = "postgresql"
		dns = dict(
			database = args.src_database, 
			host = args.src_host,
			user = args.src_user,
			password = args.src_password
		)
		if args.src_port is not None:
			dns['port'] = args.src_port
		conn = psycopg2.connect(**dns)
		c = conn.cursor()

		print "Successfully connected to postgresql db ..."
	except Exception, e:
		try:
			print "Failed to connect to postgresql db. Reason: ", e
			print "Trying mysql instead ..."
			import MySQLdb
			import _mysql_exceptions
	
			TYPE = "mysql"
			dns = dict(
				db = args.src_database, 
				host = args.src_host,
				user = args.src_user,
				passwd = args.src_password
			)
			if args.src_port is not None:
				dns["port"] = args.src_port
			conn = MySQLdb.connect(**dns)
			c = conn.cursor()
			print "Successfully connected to mysql database!"
		except Exception, e:
			try:
				print "Failed to connect to mysql database db. Reason: ", e 
				print "Trying oracle instead ..."
				import cx_Oracle
				connection_string = args.src_user + "/" + args.src_password + "@" + args.src_host + ":" + str(args.src_port) + "/" + args.src_database
				conn = cx_Oracle.Connection(connection_string)
				c = cx_Oracle.Cursor(conn)
				TYPE = "oracle"
			except Exception, e:
				print >> sys.stderr, "Could not connect to source database:", e
				sys.exit(1)
		
try:
	r = redis.Redis(host=args.dst_host, port=args.dst_port, db=args.dst_database)
except e:
	print >> sys.stderr, "Could not connect to Redis database: ", e
	sys.exit(1)
	
if args.clear_queue:
	r.delete(common.REDIS_QUEUE_KEY)
	

startTime = datetime.datetime.now()
print "%s: connected to source and destination database" % (startTime)

print "Starting to import flows beginning from ", datetime.datetime.utcfromtimestamp(args.start_time)
lastTable = None
count = 0

if TYPE == "mongodb":
	import socket
	import struct
	# we don't need anything
	conns = connections.find()
	for conn in conns:
		count += 1
		del conn["_id"]
		if "flights" in conn:
			del conn["flights"]
		# convert ip string to integers
		conn["srcIP"] = struct.unpack('!L',socket.inet_aton(conn["srcIP"]))[0]
		conn["dstIP"] = struct.unpack('!L',socket.inet_aton(conn["dstIP"]))[0]
		conn["firstSwitched"] = int(conn["firstSwitched"])
		conn["lastSwitched"] = int(conn["lastSwitched"])

		queue_length = r.rpush(common.REDIS_QUEUE_KEY, json.dumps(conn))
		while queue_length > args.max_queue:
			print "Max queue length reached, importing paused..."
			time.sleep(10)
			queue_length = r.llen(common.REDIS_QUEUE_KEY)

	# Append termination flag to queue
	# The preprocessing daemon will terminate with this flag.
	r.rpush(common.REDIS_QUEUE_KEY, "END")

	endTime = datetime.datetime.now()
	print "%s: imported %i flows in %s" % (endTime, count, endTime - startTime)
	sys.exit(0)

while True:
	# get all flow tables
	if TYPE == "oracle":
		c.execute("""SELECT * FROM user_objects WHERE object_type = 'TABLE' AND object_name LIKE 'H_%'""")
	else:
		c.execute("""SELECT table_name from information_schema.tables 
			WHERE table_schema=%s AND table_type='BASE TABLE' AND table_name LIKE 'h\\_%%' ORDER BY table_name ASC""", (args.src_database))
	print "Getting all table names ..."
	tables = c.fetchall()

	# get the table names in list format
	tables = map(lambda x: x[0], list(tables))

	if TYPE == "oracle":
		tables = sorted(tables, compareTables)
	else:
		tables.sort()

	# get the tables that contain the flows starting with args.start_time
	tables = getTables(tables, args.start_time, args.end_time, TYPE)

	if args.continuous_update:
		# TODO: this is a bit hacky. since we do not know flow timeouts and do not (yet) have unique identifiers
		# for flows in our database, we cannot know if we have already imported flows (we cannot used firstSwitched 
		# as an indicator since life inserts into the database are not ordered by firstSwitched
		# we therefore use the arbitary value of a delay of the three latest tables that we are not going to 
		# include into the mongodb. hence, we have a one hour delay when importing flows
		# FIXME: remove this hack as soon as we have unique flow identifiers
		if len(tables) > 3:
			tables = tables[0:-3]
		else:
			tables = []

	# do not import from tables that we already imported
	if lastTable != None:
		try:
			idx = tables.index(lastTable)
			if len(tables) >= idx:
				# we have no new data. sleep two minutes and try again
				time.sleep(120)
				continue
			else:
				# delete the tables that we already imported from list of tables to import
				tables = tables[idx:-1]
		except:
			raise Exception("Logic error: We already consumed table \"%s\". But we cannot find it in the list of available tables!" % (lastTable))

	print "Consuming %u new tables ..." % (len(tables))
	
	for i, table in enumerate(tables):
		lastTable = table
		if not args.continuous_update: 
			progress(100, i/len(tables)*100)

		print "Importing table ", table, "..."
	
		c.execute("SELECT * FROM " + table + " WHERE  FIRSTSWITCHED >= " + str(args.start_time) + " ORDER BY FIRSTSWITCHED ASC")
	
		for row in c:
			obj = dict()
			for j, col in enumerate(c.description):
				if col[0] not in common.IGNORE_COLUMNS:
					if TYPE == "oracle" and col[0] in common.COLUMNMAP:
						obj[common.COLUMNMAP[col[0]]] = row[j]
					else:
						obj[col[0]] = row[j]
						
		
			queue_length = r.rpush(common.REDIS_QUEUE_KEY, json.dumps(obj))
			while queue_length > args.max_queue:
				print "Max queue length reached, importing paused..."
				time.sleep(10)
				queue_length = r.llen(common.REDIS_QUEUE_KEY)
			
			count += 1
	if args.continuous_update:
		time.sleep(600)
	else:
		break
		

progress(100, 100)

# Append termination flag to queue
# The preprocessing daemon will terminate with this flag.
r.rpush(common.REDIS_QUEUE_KEY, "END")

endTime = datetime.datetime.now()
print "%s: imported %i flows in %s" % (endTime, count, endTime - startTime)

c.close()
conn.close()
