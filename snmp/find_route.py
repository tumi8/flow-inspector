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

	# find possisble source networks
	for route in ipCidrRoute.find({
		"$or": [{"ipCidrRouteProto": "2"}, {"ipCidrRouteProto": "3"}],
		"timestamp": timestamp, "low_ip": {"$lte": ip_src}, "high_ip": {"$gte": ip_src}}):

		# do not use routes to 0.0.0.0 here
		if route["ip_dst"] == 0:
			continue
		print >> sys.stderr, ("Source network: %s/%s attached to %s (%s)" % 
			(int2ip(route["ip_dst"]), route["mask_dst"],
			int2ip(route["ip_src"]), route["ipCidrRouteProto"]))
		router_to_process.append(route["ip_src"])
	
	# use last hop to default gateway as incoming router
	if len(router_to_process) == 0 and useDefaultGatewayIncoming:
		router_to_process.append(ip2int(useDefaultGatewayIncoming))
		if verbose:
			print >> sys.stderr, "No source network found, assuming incoming flow"
		return_value |= 2

	if verbose:
		print >> sys.stderr, "Destination IP: %s" % int2ip(ip_dst)
	
	# process all next hop router
	while router_to_process:
		# get next router
		router = router_to_process.popleft()

		# avoid endless loops while checking
		if router in router_done:
			continue
		
		# check whether flow passes observation point right now
		if observationPoint and int2ip(router) in observationPoint:
			return_value |= 8

		# find next routes to desination network
		for route in ipCidrRoute.find(
			spec = {"ip_src": router, "timestamp": timestamp, "low_ip": {"$lte": ip_dst}, "high_ip": {"$gte": ip_dst}},
			sort = {"mask_dst": -1},
			limit = 1 
		):
			# decide whether taking routes to 0.0.0.0 into account or not
			if route["ip_dst"] == 0 and not useDefaultGatewayOutgoing:
				continue
			
			# find router belonging to identified next hop
			result = interface_log.find({"ipAdEntAddr": route["ip_gtw"], "timestamp": timestamp})
			
			# we're done
			if len(result) == 0:
				if verbose:
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
			# identify next target
			else:
				if verbose:
					print >> sys.stderr, ("Next Hop: %s -> %s/%s via %s (belongs to %s)" %
						(int2ip(router),
						 int2ip(route["ip_dst"]),
						 route["mask_dst"],
						 int2ip(route["ip_gtw"]),
						 result[0]["router"]))
				router_to_process.append(ip2int(result[0]["router"]))
				break
		router_done.add(router)

	if verbose:
		print >> sys.stderr, "No destination network found, assuming outgoing flow"
	return return_value | 4


# TODO: won't work right now
# TODO: copy functionality from findRouteIPTable() and adopt to eigrp
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

# allow this to be used as a single programs or within another program
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

	db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_name)
	
	global ipCidrRoute, interface_log
	ipCidrRoute = db.getCollection("ipCidrRoute")
	interface_log = db.getCollection("interface_log")

	global timestamp
	if args.timestamp:
		timestamp = args.timestamp
	else:
		timestamp = sorted(ipCidrRoute.distinct("timestamp"), reverse=True)[0]
	
	print "Using IP route table information"
	findRouteIPTable(ip2int(args.src_ip), ip2int(args.dst_ip), verbose = True, useDefaultGatewayIncoming=("130.198.1.1"), useDefaultGatewayOutgoing=True)
#	print ""
#	print "Using EIGRP route information"
#	findRouteEIGRP(ip2int(args.src_ip), ip2int(args.dst_ip))

if __name__ == "__main__":
	main()
else:
	db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_snmp_name)
	ipCidrRoute = db.getCollection("ipCidrRoute")
	interface_log = db.getCollection("interface_log")
	timestamp = sorted(ipCidrRoute.distinct("timestamp"), reverse=True)[0]
