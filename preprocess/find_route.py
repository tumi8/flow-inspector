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
from collections import deque

import common
import backend
import config
import time

parser = argparse.ArgumentParser(description="Preprocess SNMP data")
parser.add_argument("source_ip")
parser.add_argument("dst_ip")
parser.add_argument("--timestamp")
parser.add_argument("--dst-host", nargs="?", default=config.db_host, help="Backend database host")
parser.add_argument("--dst-port", nargs="?", default=config.db_port, type=int, help="Backend database port")
parser.add_argument("--dst-user", nargs="?", default=config.db_user, help="Backend database user")
parser.add_argument("--dst-password", nargs="?", default=config.db_password, help="Backend database password")
parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="Backend database name")

args = parser.parse_args()

db = pymongo.Connection(args.dst_host, args.dst_port)[args.dst_database]
collection = db["snmp_raw"]

ip_source = IPAddress(args.source_ip)
ip_dst = IPAddress(args.dst_ip)

if args.timestamp:
    timestamp = args.timestamp
else:
    timestamp = sorted(collection.distinct("timestamp"), reverse=True)[0]


print "Using IP route table information"

router_to_process = deque()
router_done = set()

for route in collection.find({"type":"ipCidrRoute", "ipCidrRouteProto": "2", "timestamp": timestamp}):
    if route["ip_dst"] == "0.0.0.0":
        continue
    network_route = IPNetwork(str(route["ip_dst"]) + "/" + str(route["mask_dst"]))
    if ip_source in network_route:
        print "Source network: " + str(network_route) + " via router " + route["ip_src"]
        router_to_process.append(route["ip_src"])

while router_to_process:
    router = router_to_process.popleft()
    if router in router_done:
        continue
    for route in collection.find({"type":"ipCidrRoute", "ip_src": router, "timestamp": timestamp}).sort("mask_dst", -1):
        if route["ip_dst"] == "0.0.0.0":
            continue
        if (ip_dst in IPNetwork(str(route["ip_dst"]) + "/" + str(route["mask_dst"]))):
            result = collection.find({"type":"interface_log", "ipAdEntAddr":route["ip_gtw"], "timestamp": timestamp})
            if result.count() > 1:
                print "Suspicious IP " + route["ip_gtw"]
            elif result.count() == 0:
                print "Next Hop: " + router + " to " + str(route["ip_dst"]) + "/" + str(route["mask_dst"]) + " via " + route["ip_gtw"] + " (unknown IP, " + route["ipCidrRouteProto"] + ", " + route["ipCidrRouteType"] + ")"
            else:
                print "Next Hop: " + router + " to " + str(route["ip_dst"]) + "/" + str(route["mask_dst"]) + " via " + route["ip_gtw"] + " belongs to " + result[0]["router"]
                router_to_process.append(result[0]["router"])
            router_done.add(router)


print ""
print "Using EIGRP route information"

router_to_process = deque()
router_done = set()

for route in collection.find({"type":"eigrp", "$or": [{"cEigrpRouteOriginType": "Connected"}, {"cEigrpRouteOriginType": "Rstatic"}], "timestamp": timestamp}):
    if route["ip_dst"] == "0.0.0.0":
        continue
    network_route = IPNetwork(str(route["ip_dst"]) + "/" + str(route["cEigrpRouteMask"]))
    if ip_source in network_route:
        print "Source network found: " + str(network_route) + " via router " + route["ip_src"]
        router_to_process.append(route["ip_src"])

while router_to_process:
    router = router_to_process.popleft()
    if router in router_done:
        continue
    for route in collection.find({"type":"eigrp", "ip_src": router, "timestamp": timestamp}).sort("cEigrpRouteMask", -1):
        if route["ip_dst"] == "0.0.0.0":
            continue
        if (ip_dst in IPNetwork(str(route["ip_dst"]) + "/" + str(route["cEigrpRouteMask"]))):
            result = collection.find({"type":"interface_log", "ipAdEntAddr":route["cEigrpNextHopAddress"], "timestamp": timestamp})
            if result.count() > 1:
                print "Suspicious IP " + route["cEigrpNextHopAddress"]
            elif result.count() == 0:
                print "Next Hop: " + router + " to " + str(route["ip_dst"]) + "/" + str(route["cEigrpRouteMask"]) + " from " + router + " is " + route["cEigrpNextHopAddress"] + " (unknown IP, " + route["cEigrpRouteOriginType"] + ")"
            else:
                print "Next Hop: " + router + " to " + str(route["ip_dst"]) + "/" + str(route["cEigrpRouteMask"]) + " via " + route["cEigrpNextHopAddress"] + " belongs to " + result[0]["router"]
                router_to_process.append(result[0]["router"])
            router_done.add(router)
