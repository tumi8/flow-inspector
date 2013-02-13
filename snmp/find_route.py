#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse

from netaddr import *
from collections import deque
from common_functions import *

import common
import backend
import config
import time

def findRouteIPTable(ip_src, ip_dst, verbose=False):
	
	router_to_process = deque()
	router_done = set()
	return_value = 0

	print >> sys.stderr, "Source IP: %s" % int2ip(ip_src)

	for route in collection.find({"type": "ipCidrRoute", "$or": [{"ipCidrRouteProto": "2"}, {"ipCidrRouteProto": "3"}], "timestamp": timestamp, "low_ip": {"$lte": ip_src}, "high_ip": {"$gte": ip_src}}, {"_id": 0}):
		if route["ip_dst"] == 0:
			continue
		print >> sys.stderr, "Source network: %s/%s" % (int2ip(route["ip_dst"]), route["mask_dst"])
		router_to_process.append(route["ip_src"])


	if len(router_to_process) == 0:
		router_to_process.append(ip2int("130.198.1.1"))
		print >> sys.stderr, "No source network found, assuming incoming flow"
		return_value = return_value + 2

	print >> sys.stderr, "Destination IP: %s" % int2ip(ip_dst)
	
	while router_to_process:
		router = router_to_process.popleft()
		if router in router_done:
			continue

		for route in collection.find({"type": "ipCidrRoute", "ip_src": router, "timestamp": timestamp, "low_ip": {"$lte": ip_dst}, "high_ip": {"$gte": ip_dst}}, {"_id": 0}):
			if route["ip_dst"] == 0:
				continue
			result = collection.find({"type": "interface_log", "ipAdEntAddr": route["ip_gtw"], "timestamp": timestamp}, {"router":1, "_id":0})
			#if result.count() > 1:
				#print "Suspicious IP " + route["ip_gtw"]
			if result.count() == 0:
				print >> sys.stderr, ("Next Hop: %s -> %s/%s via %s (unknown IP, %s, %s)" %
					(int2ip(router),
					 int2ip(route["ip_dst"]),
					 route["mask_dst"],
					 int2ip(route["ip_gtw"]),
					 route["ipCidrRouteType"],
					 route["ipCidrRouteProto"]))
				return return_value + 1
			else:
				print >> sys.stderr, ("Next Hop: %s -> %s/%s via %s (belongs to %s)" %
					(int2ip(router),
					 int2ip(route["ip_dst"]),
					 route["mask_dst"],
					 int2ip(route["ip_gtw"]),
					 result[0]["router"]))
				router_to_process.append(ip2int(result[0]["router"]))
		router_done.add(router)

	print >> sys.stderr, "No destination network found, assuming outgoing flow"
	return return_value + 4


def findRouteEIGRP(ip_src, ip_dst):
	
	router_to_process = deque()
	router_done = set()

#	print ({"type": "eigrp", "$or": [{"cEigrpRouteOriginType": "Connected"}, {"cEigrpRouteOriginType": "Rstatic"}], "timestamp": timestamp, "low_ip": {"$lte": ip_src}, "high_ip": {"$gte": ip_src}})
	for route in collection.find({"type": "eigrp", "$or": [{"cEigrpRouteOriginType": "Connected"}, {"cEigrpRouteOriginType": "Rstatic"}], "timestamp": timestamp, "low_ip": {"$lte": ip_src}, "high_ip": {"$gte": ip_src}}):
		if route["ip_dst"] == 0:
			continue
		router_to_process.append(route["ip_src"])

	if len(router_to_process) == 0:
		router_to_process.append(ip2int("130.198.1.1"))
		print "Using default gateway"

	while router_to_process:
		router = router_to_process.popleft()
		if router in router_done:
			continue
		for route in collection.find({"type": "eigrp", "ip_src": router, "timestamp": timestamp, "low_ip": {"$lte": ip_dst}, "high_ip": {"$gte": ip_dst}}):
			if route["ip_dst"] == 0:
				continue
			result = collection.find({"type": "interface_log", "ipAdEntAddr": route["cEigrpNextHopAddress"], "timestamp": timestamp})
#			if result.count() > 1:
#				print "Suspicious IP " + route["cEigrpNextHopAddress"]
			if result.count() == 0:
#				print ("Next Hop: %s -> %s/%s via %s (unkown IP, %s)" %
#					(int2ip(router), int2ip(route["ip_dst"]), route["cEigrpRouteMask"],
#					int2ip(route["cEigrpNextHopAddress"]), route["cEigrpRouteOriginType"]))
				return True
			else:
#				print ("Next Hop: %s -> %s/%s via %s (belongs to %s)" %
#					(int2ip(router), int2ip(route["ip_dst"]), route["cEigrpRouteMask"],
#					int2ip(route["cEigrpNextHopAddress"]), result[0]["router"]))
				router_to_process.append(ip2int(result[0]["router"]))
		router_done.add(router)

	return False

def main():
	parser = argparse.ArgumentParser(description="Preprocess SNMP data")
	parser.add_argument("src_ip")
	parser.add_argument("dst_ip")
	parser.add_argument("--timestamp")
	parser.add_argument("--dst-host", nargs="?", default=config.data_backend_host, help="Backend database host")
	parser.add_argument("--dst-port", nargs="?", default=config.data_backend_port, type=int, help="Backend database port")
	parser.add_argument("--dst-user", nargs="?", default=config.data_backend_user, help="Backend database user")
	parser.add_argument("--dst-password", nargs="?", default=config.data_backend_password, help="Backend database password")
	parser.add_argument("--dst-database", nargs="?", default=config.data_backend_name, help="Backend database name")

	args = parser.parse_args()

	global collection
	db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_name)
	collection = db.getCollection("snmp_raw")
	
	global timestamp
	if args.timestamp:
		timestamp = args.timestamp
	else:
		timestamp = sorted(collection.distinct("timestamp"), reverse=True)[0]
	
	print "Using IP route table information"
	findRouteIPTable(ip2int(args.src_ip), ip2int(args.dst_ip))
	print ""
	print "Using EIGRP route information"
	findRouteEIGRP(ip2int(args.src_ip), ip2int(args.dst_ip))

if __name__ == "__main__":
	main()
else:
	global collecion, timestamp
	db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_name)
	collection = db.getCollection("snmp_raw")
	timestamp = sorted(collection.distinct("timestamp"), reverse=True)[0]
