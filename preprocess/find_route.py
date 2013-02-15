#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse
import pymongo

from netaddr import *
from collections import deque
from common_functions import *

import common
import backend
import config
import time

def findRouteIPTable(ip_src, ip_dst, useDefaultGatewayIncoming=None, useDefaultGatewayOutgoing=False, observationPoint=None, verbose=False):
	
	'''
		return values are intended to be read as a 4-bit binary value where
		- bit 0 indicates found / not found           -> 1
		- bit 1 indicates incoming / not incoming     -> 2
		- bit 2 indicates outgoing / not outgoing     -> 4
		- bit 3 indicates routes via observationPoint -> 8

		not found - 0000 -> 0  (flow should *NOT* be visible by observation point)
		intra     - 0001 -> 1  (flow should *NOT* be visible by observation point)
		incoming  - 0011 -> 3  (flow should *NOT* be visible by observation point)
		outgoing  - 0101 -> 5  (flow should *NOT* be visible by observation point)
		forward   - 0110 -> 6  (flow should *NOT* be visible by observation point)

		intra     - 1001 -> 9  (flow should be visible by observation point)
		incoming  - 1011 -> 11 (flow should be visible by observation point)
		outgoing  - 1101 -> 13 (flow should be visible by observation point)
		forward   - 1110 -> 14 (flow should be visible by observation point)

	'''

	# intermediate process variables
	router_to_process = deque()
	router_done = set()
	return_value = 0

	# no oberservation point defined, hence all flows are visible
	if not observationPoint:
		return_value |= 8
	
	# give verbose information to stderr
	if verbose:
		print >> sys.stderr, "Source IP: %s" % int2ip(ip_src)

	# 
	for route in collection.find({"type": "ipCidrRoute", "$or": [{"ipCidrRouteProto": "2"}, {"ipCidrRouteProto": "3"}], "timestamp": timestamp, "low_ip": {"$lte": ip_src}, "high_ip": {"$gte": ip_src}}):
		if route["ip_dst"] == 0:
			continue
		print >> sys.stderr, "Source network: %s/%s attached to %s (%s)" % (int2ip(route["ip_dst"]), route["mask_dst"], int2ip(route["ip_src"]), route["ipCidrRouteProto"])
		router_to_process.append(route["ip_src"])


	if len(router_to_process) == 0 and useDefaultGatewayIncoming:
		router_to_process.append(ip2int(useDefaultGatewayIncoming))
		if verbose:
			print >> sys.stderr, "No source network found, assuming incoming flow"
		return_value |= 2

	print >> sys.stderr, "Destination IP: %s" % int2ip(ip_dst)
	
	while router_to_process:
		router = router_to_process.popleft()
		if router in router_done:
			continue

		if observationPoint and int2ip(router) in observationPoint:
			return_value |= 8

		for route in collection.find(
			spec = {"type": "ipCidrRoute", "ip_src": router, "timestamp": timestamp, "low_ip": {"$lte": ip_dst}, "high_ip": {"$gte": ip_dst}},
			sort = [("mask_dst", -1)],
			limit = 1
		):
#			if route["ip_dst"] == 0:
#				continue
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
				if route["ip_dst"] == 0:
					return return_value | 4
				else:
					return return_value | 1
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
	return return_value | 4


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
	parser.add_argument("--dst-host", nargs="?", default=config.db_host, help="Backend database host")
	parser.add_argument("--dst-port", nargs="?", default=config.db_port, type=int, help="Backend database port")
	parser.add_argument("--dst-user", nargs="?", default=config.db_user, help="Backend database user")
	parser.add_argument("--dst-password", nargs="?", default=config.db_password, help="Backend database password")
	parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="Backend database name")

	args = parser.parse_args()

	global collection
	db = pymongo.Connection(args.dst_host, args.dst_port)[args.dst_database]
	collection = db["snmp_raw"]
	
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
	db = pymongo.Connection(config.db_host, config.db_port)[config.db_name]
	collection = db["snmp_raw"]
	timestamp = sorted(collection.distinct("timestamp"), reverse=True)[0]
