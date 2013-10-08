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
from net_functions import *
from find_route import findRouteIPTable, findRouteEIGRP

import common
import backend
import config
import time

db = pymongo.Connection(config.db_host, config.db_port)[config.db_name]
collection = db["snmp_raw"]
timestamp = sorted(collection.distinct("timestamp"), reverse=True)[0]

for router in collection.find({"type": "ipCidrRoute", "timestamp": timestamp}).distinct("ip_src"):
	findRouteIPTable(router, ip2int("0.0.0.0"))

gateways = collection.find({"type": "ipCidrRoute", "ip_dst": 0, "timestamp": timestamp}).distinct("ip_gtw")
gateways = set(gateways)

gateways_found = set()
for gateway in gateways:
	if collection.find({"ipAdEntAddr": gateway}).count() > 0:
		gateways_found.add(gateway)
gateways -= gateways_found

for gateway in gateways:
	print int2ip(gateway)
