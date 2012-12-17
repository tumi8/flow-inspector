#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse
import pymongo

from snmp_utils import Graph, Node, Router, Interface, Subnet, graph_to_graphmlfile

import common
import backend
import config

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


for entry in collection.find( { "type": "interface", "ifOperStatus": "1" } ):
   
    graph.add_router(entry["router"])
    if "ipAdEntAddr" not in entry:
        entry["ipAdEntAddr"] = entry["router"] + "jjj"
        entry["ipAdEntNetMask"] = "fuck"
    graph.add_router_interface(entry["router"], entry["ipAdEntAddr"], entry ["ifIndex"], entry["ipAdEntNetMask"])


for entry in collection.find( { "type": "route", "ipRouteType" : "3" } ):
    
    # print entry

    graph.add_router_route(entry["ip_src"], entry["ipRouteIfIndex"], entry["ipRouteNextHop"])
    graph.add_edge(entry["ipRouteNextHop"], 32, entry["ipRouteDest"], entry["ipRouteMask"])

for entry in collection.find( { "type": "route", "ipRouteType" : {"$ne": "3"} } ):
    
    graph.add_router_indirect_route(entry["ip_src"], entry["ipRouteDest"], entry["ipRouteMask"], entry["ipRouteNextHop"], entry["ipRouteIfIndex"])
              

graph_to_graphmlfile(graph, "test.route.graphml")




## create eigrp graph ##

graph = Graph()

for entry in collection.find( { "type": "interfaces" }, { "ip_src": 1, "ipAdEntAddr": 1, "ipAdEntIfIndex" : 1}):
     graph.add_router(entry["ip_src"])
     graph.add_router_interface(entry["ip_src"], entry["ipAdEntAddr"], entry ["ipAdEntIfIndex"])
    
for entry in collection.find( {"type": "eigrp"}, {"ip_src": 1, "cEigrpNextHopAddress": 1, "ip_dst": 1, "cEigrpRouteMask": 1,  "_id": 0} ):

    graph.add_edge(entry["ip_src"], 32, entry["cEigrpNextHopAddress"], 32)
    graph.add_edge(entry["cEigrpNextHopAddress"], 32, entry["ip_dst"], entry["cEigrpRouteMask"])

graph_to_graphmlfile(graph, "test.eigrp.graphml")
