#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse

from snmp_utils import Graph, Node, Router, Interface, Subnet, graph_to_graphmlfile
from find_route import findRouteIPTable, findRouteEIGRP
from copy import deepcopy
from netaddr import *
from collections import deque
from common_functions import *

import common
import backend
import config
import time

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
#	"end_bucket": min_bucket + 1000,
	"end_bucket": max_bucket,
	"biflow": False,
	"include_ports": "",
	"exclude_ports": "",
	"include_ips": "",
	"exclude_ips": "",
	"limit": None,
	"bucket_size": "",
	"include_protos": "",
	"exclude_protos": "",
	"batch_size": "",
	"aggregate": None,
	"black_others": "",

	"resolution": ""
})

flows_processed = 0
routes_found_ip = 0
routes_found_eigrp = 0
routes_not_found = 0
routes_incoming = 0
routes_outgoing = 0 
routes_intra = 0
routes_forward = 0
routes_invisible = 0

print "Total flows: %s" % len(result[0])

begin = time.time()
last = begin

f = open('check_report.txt', 'w')

f_not_found = open('flows_not_found.txt', 'w')
f_incoming = open('flows_incoming.txt', 'w')
f_outgoing = open('flows_outgoing.txt', 'w')
f_intra = open('flows_intra.txt', 'w')
f_forward = open('flows_forward.txt', 'w')
f_invisible = open('flows_invisible.txt', 'w')

for flow in result[0]:
	
	ip_table = findRouteIPTable((flow["sourceIPv4Address"]), (flow["destinationIPv4Address"]), observationPoint = ["130.198.1.1", "130.198.1.2"], verbose = True, useDefaultGatewayIncoming = "130.198.1.1", useDefaultGatewayOutgoing = True)
	if ip_table > 0:
		kind = "this text is not supposed to be ever printed"
		if ip_table == 1:
			routes_invisible = routes_invisible + 1
			f_invisible.write("%s -> %s (intra)\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "intra"
		elif ip_table == 3:
			routes_invisible = routes_invisible + 1
			f_invisible.write("%s -> %s (incoming)\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "incoming"
		elif ip_table == 4:
			routes_invisible = routes_invisible + 1
			f_invisible.write("%s -> %s (outgoing)\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "outgoing"
		elif ip_table == 6:
			routes_invisible = routes_invisible + 1
			f_invisible.write("%s -> %s (forward)\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "forward"
		elif ip_table == 9:
			routes_intra = routes_intra + 1
			f_intra.write("%s -> %s\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "intra"
		elif ip_table == 11:
			routes_incoming = routes_incoming + 1
			f_incoming.write("%s -> %s\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "incoming"
		elif ip_table == 12:
			routes_outgoing = routes_outgoing + 1
			f_outgoing.write("%s -> %s\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "outgoing"
		elif ip_table == 14:
			routes_forward = routes_forward + 1
			f_forward.write("%s -> %s\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))
			kind = "forward"
		else:
			print "stupid mistake!!!!!!!! " + str(ip_table)
			sys.exit(-1000)
		routes_found_ip = routes_found_ip + 1
		f.write("%s -> %s found in IPTable (%s)\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"]), kind))
	
	#eigrp = findRouteEIGRP((flow["sourceIPv4Address"]), (flow["destinationIPv4Address"]))
	eigrp = False
	if eigrp:
		routes_found_eigrp = routes_found_eigrp + 1
		f.write("%s -> %s found in EIGRP\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))

	if ip_table == 0:
		routes_not_found = routes_not_found + 1
		f.write("%s -> %s not found\n" % (int2ip(flow["sourceIPv4Address"]), int2ip(flow["destinationIPv4Address"])))

	flows_processed = flows_processed + 1

	current = time.time()
	if current - last > 5:
		print "Processed %s flows in %s seconds (%s flows per second)" % (flows_processed, current - begin, flows_processed / (current - begin))
		last = current

f.close()

f_not_found.close()
f_incoming.close()
f_outgoing.close()
f_intra.close()
f_forward.close()
f_invisible.close()

print "Flows processed: %i" % flows_processed
print "Routes found IP: %i" % routes_found_ip
print "Routes found EIGRP: %i" % routes_found_eigrp
print "Routes not found: %s" % routes_not_found
print "Routes incoming: %s" % routes_incoming
print "Routes outgoing: %s" % routes_outgoing
print "Routes intra: %s" % routes_intra
print "Routes forward: %s" % routes_forward
print "Routes invisible: %s" % routes_invisible

