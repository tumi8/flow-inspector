#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
import argparse
import time
import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))

import config
import common
import backend

from common_functions import *

# dictionary which maps oid -> name and fct to parse oid value
oidmap = readDictionary("oidmap.csv")

# dictionary containing table descriptions
fieldDict = readDictionary("fieldDict.csv")
fieldDictOracle = readDictionary("fieldDictOracle.csv")

def parse_snmp_file(file, doc):
	# parse file name
	params = os.path.basename(file).rstrip(".txt").split("-")
	source_type = params[0]
	ip_src = params[1]
	timestamp = params[2]
	lines = 0

	# read and process file contents
	file = open(file, "r")
	for line in file:
		index = line.find(" ")
		value = line[index + 1:]
		value = value.strip("\n")
		value = value.strip('"')
		line = line[0:index]

		# parse interface_phy oid
		if line.startswith(".1.3.6.1.2.1.2.2.1"):
			line = line.split(".")
			oid = '.'.join(line[0:11])
			interface = line[11]

			if oid in oidmap:
				update_doc(
					doc,
					"interface_phy",
					ip_src + '-' + interface + '-' + timestamp,
					{"router": ip_src, "if_number": interface,
						"timestamp": timestamp},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse interface_log oid
		elif line.startswith(".1.3.6.1.2.1.4.20.1"):
			line = line.split(".")
			oid = '.'.join(line[0:11])
			ip = '.'.join(line[11:15])

			if oid in oidmap:
				update_doc(
					doc,
					"interface_log",
					ip_src + '-' + ip + '-' + timestamp,
					{"router": ip_src, "if_ip": ip2int(ip),
						"timestamp": timestamp},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse ip route oid
		elif line.startswith(".1.3.6.1.2.1.4.21.1"):
			line = line.split(".")
			oid = '.'.join(line[0:11])
			ip = '.'.join(line[11:15])

			if oid in oidmap:
				update_doc(
					doc,
					"ipRoute",
					ip_src + '-' + ip + '-' + timestamp,
					{"ip_src": ip2int(ip_src), "timestamp": timestamp,
						"ip_dst": ip2int(ip)},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse eigrp oid
		elif line.startswith(".1.3.6.1.4.1.9.9.449.1.3.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			ip = '.'.join(line[19:23])

			if oid in oidmap:
				update_doc(
					doc,
					"cEigrp",
					ip_src + '-' + ip + '-' + line[23] + '-' + timestamp,
					{"ip_src": ip2int(ip_src), "timestamp": timestamp,
					"ip_dst": ip2int(ip), "mask_dst": line[23]},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse ipcidrroute oid
		elif line.startswith(".1.3.6.1.2.1.4.24.4.1"):
			line = line.split(".")
			oid = '.'.join(line[0:12])
			ip_dst = '.'.join(line[12:16])
			mask_dst = '.'.join(line[16:20])
			ip_gtw = '.'.join(line[21:25])

			if oid in oidmap:
				update_doc(
					doc,
					"ipCidrRoute",
					ip_src + '-' + ip_dst + '-' + mask_dst + '-' + ip_gtw + '-' + timestamp,
					{"ip_src": ip2int(ip_src), "timestamp": timestamp, "ip_dst": ip2int(ip_dst),
						"mask_dst": netmask2int(mask_dst), "ip_gtw": ip2int(ip_gtw)},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse ifxtable oid
		elif line.startswith(".1.3.6.1.2.1.31.1.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:12])
			if_number = line[12]

			if oid in oidmap:
				update_doc(
					doc,
					"ifXTable",
					ip_src + '-' + if_number + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "if_number": if_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse juniperLoadbalancer session name
		elif line.startswith(".1.3.6.1.4.1.6213.2.4.2.5.1.2"):
			line = line.split(".")
			oid = '.'.join(line[0:14])
			session_number = line[14]

			if oid in oidmap:
				update_doc(
					doc,
					"juniperLoadbalancer",
					ip_src + '-' + session_number + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "session_number": session_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse juniperLoadbalancer session counter
		elif line.startswith(".1.3.6.1.4.1.6213.2.4.2.1.1.2"):
			line = line.split(".")
			oid = '.'.join(line[0:14])
			session_number = line[14]

			if oid in oidmap:
				update_doc(
					doc,
					"juniperLoadbalancer",
					ip_src + '-' + session_number + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "session_number": session_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse cisco cluster name
		elif line.startswith(".1.3.6.1.4.1.9.9.368.1.15.2.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			scm_number = line[15]
			ident = '.'.join(line[16:])

			if oid in oidmap:
				update_doc(
					doc,
					"cssLoadbalancer",
					ip_src + '-' + scm_number + '-' + ident + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "scm_number": scm_number, "ident": ident},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# aprse cisco cluster session count
		elif line.startswith(".1.3.6.1.4.1.9.9.368.1.15.2.1.20"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			scm_number = line[15]
			ident = '.'.join(line[16:])

			if oid in oidmap:
				update_doc(
					doc,
					"cssLoadbalancer",
					ip_src + '-' + scm_number + '-' + ident + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "scm_number": scm_number, "ident": ident},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse cisco cpu
		elif line.startswith(".1.3.6.1.4.1.9.9.109.1.1.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			cpu_number = line[15]

			if oid in oidmap:
				update_doc(
					doc,
					"ciscoCpu",
					ip_src + '-' + cpu_number + '-' + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "cpu_number": cpu_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)


		# increment counter for processed lines
		lines += 1

	return (lines, timestamp, doc)

def getFieldDict(args):
	if args.backend == "mysql":
		return fieldDict
	elif args.backend == "oracle":
		return fieldDictOracle
	elif args.backend == "mongo":
		return None
	else:
		raise Exception("Unknown data backend: " + args.backend);

def update_doc(doc, table, table_key, db_key, db_values):
	""" update local document before comitting to databackend """
	if not table in doc:
		doc[table] = dict()
	if table_key in doc[table]:
		doc[table][table_key][1]["$set"].update(db_values)
	else:
		doc[table][table_key] = (db_key, {"$set": db_values})


def commit_doc(doc, collections):
	time_begin = time.time()
	time_last = time_begin
	time_current = 0
	counter = 0
	total = sum(len(doc[table]) for table in doc)

	print "Commiting %s entries to databackend" % total
	
	for name, table in doc.items():
		# push into the database
		print name
		for value in table.itervalues():
			collections[name].update(value[0], value[1], True)
			counter = counter + 1
		time_current = time.time()
		if (time_current - time_last > 5):
			print "Processed {0} entries in {1} seconds ({2} entries per second, {3}% done)".format(
				counter, time_current - time_begin,
				counter / (time_current - time_begin), 100.0 * counter / total)
			time_last = time_current 
	doc = {}

def main():
	doc = {}
	collections = dict()


	parser = argparse.ArgumentParser(description="Parse SNMP data files and import data to database")
	parser.add_argument("data_path", help="Path to the data that should be inserted. This must be a file if neither -d or -r are given.")
	parser.add_argument("-d", "--directory", action="store_true", help="Parse directory instead of a single file. The directory will be scanned for <directory>/*.txt:")
	parser.add_argument("-r", "--recursive", action="store_true", help="Recurse direcory, i.e. expecting files in <directory>/*/*.txt")
	parser.add_argument("--dst-host", nargs="?", default=config.data_backend_host, help="Backend database host")
	parser.add_argument("--dst-port", nargs="?", default=config.data_backend_port, type=int, help="Backend database port")
	parser.add_argument("--dst-user", nargs="?", default=config.data_backend_user, help="Backend database user")
	parser.add_argument("--dst-password", nargs="?", default=config.data_backend_password, help="Backend database password")
	parser.add_argument("--dst-database", nargs="?", default=config.data_backend_snmp_name, help="Backend database name")
	parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole databse before importing any flows.")
	parser.add_argument( "--backend", nargs="?", default=config.data_backend, const=True, help="Selects the backend type that is used to store the data")


	args = parser.parse_args()

	dst_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database)

	if args.clear_database:
		dst_db.clearDatabase()

	for name, fields in getFieldDict(args).items():
		dst_db.prepareCollection(name, fields)
		collections[name] = dst_db.getCollection(name)
	
	# TODO: hacky ... make something more general ...
	if backend == "mongo":
		db = pymongo.Connection(args.dst_host, args.dst_port)[args.dst_database]
		collection = db["snmp_raw"]
		collection.ensure_index([("router", pymongo.ASCENDING), ("if_number", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])
		collection.ensure_index([("router", pymongo.ASCENDING), ("if_ip", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])
		collection.ensure_index([("ip_src", pymongo.ASCENDING), ("ip_dst", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])
		collection.ensure_index([("ip_src", pymongo.ASCENDING), ("ip_dst", pymongo.ASCENDING), ("mask_dst", pymongo.ASCENDING), ("ip_gtw", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])

		# restore generic backend collection
		collection = dst_db.getCollection("snmp_raw")
	else: 
	#	collection.createIndex("router")
	#	collection.createIndex("if_number")
	#	collection.createIndex("timestamp")
	#	collection.createIndex("type")
	#	collection.createIndex("ip_src")
	#	collection.createIndex("ip_dst")
		pass

	# enviromental settings
	cache_treshold = 10000000

	# statistical counters
	time_begin = time.time()
	time_last = time_begin
	counter = 0

	# local document storage
	lines_since_commit = 0
	timestamps = set()

	
	# TODO: implies precedence of operators, maybe something better can be done here
	if args.directory:
		files = glob.glob(args.data_path + "/*.txt")
	elif args.recursive:
		files = glob.glob(args.data_path + "/*/*.txt")
	else:
		files = [ args.data_path ]
	
	for file in files:
			(read_lines, timestamp, doc) = parse_snmp_file(file, doc)
			lines_since_commit += read_lines
			counter += read_lines
			timestamps.add(timestamp)
			if lines_since_commit > cache_treshold:
				commit_doc(doc, collections)
				lines_since_commit = 0

			# do statistical calculation
			time_current = time.time()
			if (time_current - time_last > 5):
				print "Processed %s lines in %s seconds (%s lines per second)" % (
					counter, time_current - time_begin, counter / (time_current - time_begin))
				time_last = time_current

	
	#	print "counter: %s" % counter


	# commit local doc to databackend in the end
	
	commit_doc(doc, collections)

	for collection in collections.itervalues():
		collection.flushCache()

	print "Calculating IP ranges"

	# calculate ip network ranges
	for timestamp in timestamps:
		for row in collections["ipCidrRoute"].find({"timestamp": timestamp}):
			(low_ip, high_ip) = calc_ip_range(row["ip_dst"], row["mask_dst"])
			collections["ipCidrRoute"].update({"_id": row["_id"]}, {"$set": {"low_ip": low_ip, "high_ip": high_ip}}, True)
	
		for row in collections["cEigrp"].find({"timestamp": timestamp}):
			(low_ip, high_ip) = calc_ip_range(row["ip_dst"], int(row["mask_dst"]))
			collections["cEigrp"].update({"_id": row["_id"]}, {"$set": {"low_ip": low_ip, "high_ip": high_ip}}, True)

	for collection in collections.itervalues():
		collection.flushCache()

	# do some statistics in the end
	time_current = time.time()
	print "Processed %s lines in %s seconds (%s lines per second)" % (
			counter, time_current - time_begin, counter / (time_current - time_begin))

if __name__ == "__main__":
	main()
