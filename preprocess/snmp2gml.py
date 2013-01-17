#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse
import pymongo

from snmp_utils import Graph, Node, Router, Interface, Subnet, graph_to_graphmlfile
from copy import deepcopy
from netaddr import *

import common
import backend
import config
import time

parser = argparse.ArgumentParser(description="Preprocess SNMP data")
parser.add_argument("--dst-host", nargs="?", default=config.db_host, help="Backend database host")
parser.add_argument("--dst-port", nargs="?", default=config.db_port, type=int, help="Backend database port")
parser.add_argument("--dst-user", nargs="?", default=config.db_user, help="Backend database user")
parser.add_argument("--dst-password", nargs="?", default=config.db_password, help="Backend database password")
parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="Backend database name")
parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole database before importing any flows.")
parser.add_argument("--backend", nargs="?", default=config.db_backend, const=True, help="Selects the backend type that is used to store the data")

args = parser.parse_args()

db = pymongo.Connection(args.dst_host, args.dst_port)[args.dst_database]
collection = db["snmp_raw"]


## create ip route graph ##

graph = Graph()

print "Creating router and interfaces"

for entry in collection.find( { "type": "interface_log" } ):
    """ add interfaces and routers to graph """   
    
    ## filter for secondary ip -- quick and dirty ##
#    if entry["ipAdEntAddr"] in ["130.197.14.254", 
#        "130.197.46.252",
#        "130.197.36.252",
#        "130.197.37.252",
#        "130.197.38.252",
#        "130.197.39.252",
#        "130.197.36.254",
#        "130.197.37.254",
#        "130.197.38.254",
#        "130.197.39.254",
#        "130.197.46.254",
#        "130.197.24.252",
#        "130.197.24.254",
#        "130.197.17.252",
#        "130.197.17.254",
#        "130.197.47.252",
#        "130.197.47.254",
#        "130.197.97.252",
#        "130.197.98.252",
#        "130.197.99.252",
#        "130.197.97.254",
#        "130.197.98.254",
#        "130.197.99.254",
#        "130.197.117.252",
#        "130.197.117.254",
#        "130.197.219.12",
#        "130.197.219.14",
#        "130.197.2.252",
#        "130.197.2.254"]:
#            print "secondary address found " + entry["ipAdEntAddr"]
#            continue 
    
    
    if_phy_info = collection.find( { 
        "type":"interface_phy", "router": entry["router"], "ifIndex": entry["ipAdEntIfIndex"]
    } )[0]

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


print "Creating route.connected"

for entry in collection.find( { "type": "route", "ipRouteType" : "3" } ):
    """ add direct routes """

    graph.addConnectedSubnet(
        entry["ip_src"],
        entry["ipRouteNextHop"],
        entry["ipRouteDest"],
        entry["ipRouteMask"],
        str(entry)
    )

graph_to_graphmlfile(graph, "ba.route.connected.graphml")


graph = deepcopy(graph_copy)

print "Creating route.local"

# parse local / direct routes
for entry in collection.find( { "type": "route", "ipRouteProto" : "2", "ipRouteType" : "3" } ):

    graph.addConnectedSubnet(
        entry["ip_src"],
        entry["ipRouteNextHop"],
        entry["ipRouteDest"],
        entry["ipRouteMask"],
        str(entry)
    )

# parse local / indirect route
for entry in collection.find( { "type": "route", "ipRouteProto" : "2", "ipRouteType" : "4" } ):

    # determine interface to reach the new router (aka longest prefix matching)
    
    router_ip = IPAddress(entry["ipRouteNextHop"])
    interface_ip = None
    interface_netmask = None
    interface_netaddress = None
    
    for interface in (collection.find({"type": "interface_log", "router": entry["ip_src"]})
                               .sort("ipAdEntNetMask", -1)):
        interface_network = IPNetwork(str(interface["ipAdEntAddr"]) + "/" + str(interface["ipAdEntNetMask"]))
        if (router_ip in interface_network):
            interface_netmask = interface["ipAdEntNetMask"]
            interface_ip = interface["ipAdEntAddr"]
            interface_netaddress = interface_network.network
            break
           

    print str(router_ip) + " in " + str(interface_network)

    if not graph.isInterface(entry["ip_src"], interface_ip):
        print "Blaaaaaa"
            
    if graph.isSubnet(interface_netaddress, interface_netmask):
        graph.addRoute_Subnet2Node(interface_netaddress, interface_netmask,
                                   entry["ipRouteNextHop"], 32)
    else:
        graph.addRoute_If2Node(entry["ip_src"], interface_ip,
                               entry["ipRouteNextHop"], 32, "55555")

#    graph.addRoute_If2Node(entry["ip_src"], interface_ip, 
#                           entry["ipRouteNextHop"], 32, "55555")

#    graph.addSubnet(entry["ipRouteDest"], entry["ipRouteMask"], str(entry))

    if entry["ipRouteMask"] < 32:
        graph.addRoute_Node2Subnet(entry["ipRouteNextHop"], "32", entry["ipRouteDest"], entry["ipRouteMask"])
    else:
        graph.addRoute_Node2Node(entry["ipRouteNextHop"], "32", entry["ipRouteDest"], entry["ipRouteMask"])

graph_to_graphmlfile(graph, "ba.route.local.graphml")

## create eigrp grahp ##

graph = deepcopy(graph_copy)

print "Creating eigrp"

for entry in collection.find( { "type":"eigrp", "cEigrpRouteOriginType":"Connected" } ):
    """ add direct routes """

    if_phy_info = collection.find( {
        "type":"interface_phy", "router": entry["ip_src"], "ifDescr": entry["cEigrpNextHopInterface"]
    } )[0]

    # print if_phy_info
    
    if_log_info = collection.find( {
        "type":"interface_log", "router": entry["ip_src"], "ipAdEntIfIndex": if_phy_info["ifIndex"]
    } )[0]


    graph.addConnectedSubnet(
        entry["ip_src"],
        if_log_info["ipAdEntAddr"],
        entry["ip_dst"],
        entry["cEigrpRouteMask"],
        str(entry)
    )

graph_to_graphmlfile(graph, "ba.eigrp.graphml")
