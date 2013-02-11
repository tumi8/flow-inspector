#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse
import pymongo

from snmp_utils import Graph, Node, Router, Interface, Subnet, graph_to_graphmlfile
from find_route import findRouteIPTable, findRouteEIGRP
from copy import deepcopy
from netaddr import *
from collections import deque

import common
import backend
import config
import time


def ip2int(ip):
	""" convert ip to int """
	ip = ip.split('.')
	return (int(ip[0]) * (2 ** 24) + int(ip[1]) * (2 ** 16) +
	        int(ip[2]) * (2 ** 8) + int(ip[3]))


def int2ip(i):
	""" convert int to ip """
	return (str(i // (2 ** 24)) + "." + str((i // (2 ** 16)) % 256) + "." +
	        str((i // (2 ** 8)) % 256) + "." + str(i % 256))


parser = argparse.ArgumentParser(description="Check routes")
parser.add_argument("--timestamp")

args = parser.parse_args()

db = backend.flowbackend.getBackendObject(config.db_backend, config.db_host, config.db_port, config.db_user, config.db_password, config.db_name)

min_bucket = db.getMinBucket()
max_bucket = db.getMaxBucket()

print min_bucket
print max_bucket

result = db.index_query("flows_600",
{
	"fields": ["sourceIPv4Address", "destinationIPv4Address"],
	"sort": "",
	"count": "",
	"start_bucket": min_bucket,
	"end_bucket": max_bucket,
	"biflow": False,
	"include_ports": "",
	"exclude_ports": "",
	"include_ips": "",
	"exclude_ips": "",
	"limit": ""
})

flows_processed = 0
routes_found_ip = 0
routes_found_eigrp = 0
routes_not_found = 0

print "Total flows: %s" % len(result[0])

begin = time.time()
last = begin

file_not_found = open('flows_not_found.txt', 'w')

for flow in result[0]:
	
	ip_table = findRouteIPTable((flow["sourceIPv4Address"]), (flow["destinationIPv4Address"]))
	if ip_table:
		routes_found_ip = routes_found_ip + 1
	
	eigrp = findRouteEIGRP((flow["sourceIPv4Address"]), (flow["destinationIPv4Address"]))
	if eigrp:
		routes_found_eigrp = routes_found_eigrp + 1

	if (not ip_table) and (not eigrp):
		routes_not_found = routes_not_found + 1
		file_not_found.write("%s -> %s\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))

	flows_processed = flows_processed + 1

	current = time.time()
	if current - last > 5:
		print "Processed %s flows in %s seconds (%s flows per second)" % (flows_processed, current - begin, flows_processed / (current - begin))
		last = current

file_not_found.close()

print "Flows processed: %i" % flows_processed
print "Routes found IP: %i" % routes_found_ip
print "Routes found EIGRP: %i" % routes_found_eigrp
print "Routes not found: %s" % routes_not_found

