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


for entry in collection.find( { "type": "interface_log" } ):
    """ add interfaces and routers to graph """   
    
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


for entry in collection.find( { "type": "route", "ipRouteType" : "3" } ):
    """ add direct routes """

    graph.addConnectedSubnet(
        entry["ip_src"],
        entry["ipRouteNextHop"],
        entry["ipRouteDest"],
        entry["ipRouteMask"],
        str(entry)
    )

graph_to_graphmlfile(graph, "test.route.connected.graphml")


graph = deepcopy(graph_copy)

for entry in collection.find( { "type": "route", "ipRouteProto" : "2" } ):
    """ add direct routes """

    graph.addConnectedSubnet(
        entry["ip_src"],
        entry["ipRouteNextHop"],
        entry["ipRouteDest"],
        entry["ipRouteMask"],
        str(entry)
    )

graph_to_graphmlfile(graph, "test.route.local.graphml")


## create eigrp grahp ##

graph = deepcopy(graph_copy)

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
        entry["cEigrpRouteMask"]
    )

graph_to_graphmlfile(graph, "test.eigrp.graphml")
