#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse

from snmp_utils import Graph, Node, Router, Interface, Subnet, graph_to_graphmlfile
from copy import deepcopy
from netaddr import *

import common
import backend
import config
import time

parser = argparse.ArgumentParser(description="Preprocess SNMP data")
parser.add_argument("--timestamp")
parser.add_argument("--dst-host", nargs="?", default=config.data_backend_host, help="Backend database host")
parser.add_argument("--dst-port", nargs="?", default=config.data_backend_port, type=int, help="Backend database port")
parser.add_argument("--dst-user", nargs="?", default=config.data_backend_user, help="Backend database user")
parser.add_argument("--dst-password", nargs="?", default=config.data_backend_password, help="Backend database password")
parser.add_argument("--dst-database", nargs="?", default=config.data_backend_snmp_table, help="Backend database name")
parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole database before importing any flows.")
parser.add_argument("--backend", nargs="?", default=config.data_backend, const=True, help="Selects the backend type that is used to store the data")

args = parser.parse_args()

dst_db = backend.databackend.getBackendObject(
        args.data_backend, args.data_backend_host, args.data_backend_port,
	args.data_backend_user, args.data_backend_password, args.data_backend_database)

collection = dst_db.getCollection("snmp_raw")

if args.timestamp:
	timestamp = args.timestamp
else:
	timestamp = sorted(collection.distinct("timestamp"), reverse=True)[0]

def do_checks(graph):
	print "Checking for interfaces without successor"
	for interface in graph.db["Interface"].itervalues():
		if len(interface.successors) == 0:
			print "Interface ohne Nachfolger: " + str(interface)

	print "Checking for unknown nodes"
	for node in graph.db["Node"].itervalues():
		if node.__class__ == Node:
			print "Unknown Node: " + str(node)

	print "Checking for wrongly assigned next hop nodes"
	for node in graph.db["Node"].itervalues():
		if node.__class__ == Node:
			for interface in graph.db["Interface"].itervalues():
				for ip in interface.ip:
					if node.ip == ip.split("/")[0]:
						print str(node.ip) + "/" + str(node.netmask) + " is not(!) unknown, belongs to " + str(interface) 
	
	print "Checking for self-loop-nodes"
	for node in graph.db["Node"].itervalues():
		if node in node.successors:
			print "Self-loop-node " + str(node)

## create ip route graph ##

graph = Graph()

print "Creating router and interfaces"

for entry in collection.find( { "type": "interface_log", "timestamp": timestamp } ):
	""" add interfaces and routers to graph """   
	
	if_phy_info = collection.find( { 
		"type":"interface_phy", "router": entry["router"], "ifIndex": entry["ipAdEntIfIndex"], "timestamp": timestamp
	} )[0]
   
	if entry["ipAdEntIfIndex"] == "375":
		 print entry

	if if_phy_info["ifOperStatus"] == "1":
		graph.addInterface(
			entry["router"],
			entry["ipAdEntAddr"],
			entry["ipAdEntNetMask"],
			entry["ipAdEntIfIndex"],
			if_phy_info["ifDescr"],
			str(entry)
		)

graph_copy = deepcopy(graph)


print "Creating route.local"

# parse local / direct routes
for entry in collection.find( { "type": "ipCidrRoute", "ipCidrRouteProto" : "2", "ipCidrRouteType" : "3", "timestamp": timestamp } ):

	graph.addConnectedSubnetByNumber(
		entry["ip_src"],
		entry["ipCidrRouteIfIndex"],
		entry["ip_dst"],
		entry["mask_dst"],
		str(entry)
	)

# parse local / indirect route
for entry in collection.find( { "type": "ipCidrRoute", "ipCidrRouteProto" : "3", "ipCidrRouteType" : "4", "timestamp": timestamp } ):

	# determine interface to reach the new router (aka longest prefix matching)
	
	router_ip = IPAddress(entry["ip_gtw"])
	interface_number = None
	interface_netmask = None
	interface_netaddress = None
	
	print "getting interface"
	interfaces = collection.find({"type": "interface_log", "router": entry["ip_src"]}, sort={"ipAdEntNetMask": -1})
 
	for interface in interfaces:
		interface_network = IPNetwork(str(interface["ipAdEntAddr"]) + "/" + str(interface["ipAdEntNetMask"]))
		if (router_ip in interface_network):
			interface_netmask = interface["ipAdEntNetMask"]
			interface_number = interface["ipAdEntIfIndex"]
			interface_netaddress = interface_network.network
			break
			
	if graph.isSubnet(interface_netaddress, interface_netmask):
		graph.addRoute_Subnet2Node(interface_netaddress, interface_netmask,
								   entry["ip_gtw"], 32)
	else:
		graph.addRoute_If2Node(entry["ip_src"], interface_number,
							   entry["ip_gtw"], 32, "55555")

	if int(entry["mask_dst"]) < 32:
		graph.addRoute_Node2Subnet(entry["ip_gtw"], "32", entry["ip_dst"], entry["mask_dst"])
	else:
		graph.addRoute_Node2Node(entry["ip_gtw"], "32", entry["ip_dst"], entry["mask_dst"])

do_checks(graph)
graph_to_graphmlfile(graph, "ba.route.local.graphml")

## create eigrp grahp ##

graph = deepcopy(graph_copy)

print "Creating eigrp"

for entry in collection.find( { "type":"eigrp", "cEigrpRouteOriginType":"Connected" } ):
	""" add direct routes """

	if_phy_info = collection.find( {
		"type":"interface_phy", "router": entry["ip_src"], "ifDescr": entry["cEigrpNextHopInterface"]
	} )[0]

	if_log_info = collection.find( {
		"type":"interface_log", "router": entry["ip_src"], "ipAdEntIfIndex": if_phy_info["ifIndex"]
	} )[0]

	graph.addConnectedSubnetByNumber(
		entry["ip_src"],
		if_log_info["ipAdEntIfIndex"],
		entry["ip_dst"],
		entry["cEigrpRouteMask"],
		str(entry)
	)

# parse RStatic routes
for entry in collection.find( { "type":"eigrp", "cEigrpRouteOriginType":"Rstatic"} ):
	
	# determine interface to reach the new router (aka longest prefix matching)
	router_ip = IPAddress(entry["cEigrpNextHopAddress"])
	interface_number = None
	interface_netmask = None
	interface_netaddress = None
	
	for interface in (collection.find({"type": "interface_log", "router": entry["ip_src"]}, sort={"ipAdEntNetMask", -1})):
		interface_network = IPNetwork(str(interface["ipAdEntAddr"]) + "/" + str(interface["ipAdEntNetMask"]))
		if (router_ip in interface_network):
			interface_netmask = interface["ipAdEntNetMask"]
			interface_number = interface["ipAdEntIfIndex"]
			interface_netaddress = interface_network.network
			break
			
	if graph.isSubnet(interface_netaddress, interface_netmask):
		graph.addRoute_Subnet2Node(interface_netaddress, interface_netmask,
								   entry["cEigrpNextHopAddress"], 32)
	else:
		graph.addRoute_If2Node(entry["ip_src"], interface_number,
							   entry["cEigrpNextHopAddress"], 32, "55555")

	if int(entry["cEigrpRouteMask"]) < 32:
		graph.addRoute_Node2Subnet(entry["cEigrpNextHopAddress"], "32", entry["ip_dst"], entry["cEigrpRouteMask"])
	else:
		graph.addRoute_Node2Node(entry["cEigrpNextHopAddress"], "32", entry["ip_dst"], entry["cEigrpRouteMask"])

do_checks(graph)
graph_to_graphmlfile(graph, "ba.eigrp.graphml")

