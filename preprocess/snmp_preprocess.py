#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import argparse

import common
import backend
import config

parser = argparse.ArgumentParser(description="Preprocess SNMP data")
parser.add_argument("file", help="File to parse")
parser.add_argument("--dst-host", nargs="?", default=config.db_host, help="Backend database host")
parser.add_argument("--dst-port", nargs="?", default=config.db_port, type=int, help="Backend database port")
parser.add_argument("--dst-user", nargs="?", default=config.db_user, help="Backend database user")
parser.add_argument("--dst-password", nargs="?", default=config.db_password, help="Backend database password")
parser.add_argument("--dst-database", nargs="?", default=config.db_name, help="Backend database name")
parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole databse before importing any flows.")
parser.add_argument("--backend", nargs="?", default=config.db_backend, const=True, help="Selects the backend type that is used to store the data")

args = parser.parse_args()

dst_db = backend.flowbackend.getBackendObject(args.backend, args.dst_host, args.dst_port, args.dst_user, args.dst_password, args.dst_database)

if args.clear_database:
	dst_db.clearDatabase()

dst_db.prepareCollections()

collection = dst_db.getCollection("test_snmp");

oid2name = {
	".1.3.6.1.2.1.4.21.1.1":  "ipRouteDest",
	".1.3.6.1.2.1.4.21.1.2":  "ipRouteIfIndex",
	".1.3.6.1.2.1.4.21.1.3":  "ipRouteMetric1",
	".1.3.6.1.2.1.4.21.1.4":  "ipRouteMetric2",
	".1.3.6.1.2.1.4.21.1.5":  "ipRouteMetric3",
	".1.3.6.1.2.1.4.21.1.6":  "ipRouteMetric4",
	".1.3.6.1.2.1.4.21.1.7":  "ipRouteNextHop",
	".1.3.6.1.2.1.4.21.1.8":  "ipRouteType",
	".1.3.6.1.2.1.4.21.1.9":  "ipRouteProto",
	".1.3.6.1.2.1.4.21.1.10": "ipRouteAge",
	".1.3.6.1.2.1.4.21.1.11": "ipRouteMask",
	".1.3.6.1.2.1.4.21.1.12": "ipRouteMetric5",
	".1.3.6.1.2.1.4.21.1.13": "ipRouteInfo",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.1":  "cEigrpDestNetType",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.2":  "cEigrpDestNet",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.3":  "cEigrpDestNetPrefixLen",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.4":  "cEigrpDestNetPrefixLen",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.5":  "cEigrpActive",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.6":  "cEigrpStuckInActive",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.7":  "cEigrpDestSuccessors",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.8":  "cEigrpFdistance",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.9":  "cEigrpRouteOriginType",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.10": "cEigrpRouteOriginAddrType",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.11": "cEigrpRouteOriginAddr",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.12": "cEigrpNextHopAddressType",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.13": "cEigrpNextHopAddress",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.14": "cEigrpNextHopInterface",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.15": "cEigrpDistance",
	".1.3.6.1.4.1.9.9.449.1.3.1.1.16": "cEigrpReportDistance"
}

def plain(value):
	return value

def hex2ip(value):
	value = value.strip(" ")
	return '.'.join(str(int(part, 16)) for part in [value[0:2], value[3:5], value[6:8], value[9:11]])

def int2netmask(value):
	value = '1' * int(value) + '0' * (32 - int(value))
	return '.'.join(str(int(part, 2)) for part in [value[0:8], value[8:16], value[16:24], value[24:32]])

oid2parsefct = {
        ".1.3.6.1.2.1.4.21.1.1":  plain,
        ".1.3.6.1.2.1.4.21.1.2":  plain,
        ".1.3.6.1.2.1.4.21.1.3":  plain,
        ".1.3.6.1.2.1.4.21.1.4":  plain,
        ".1.3.6.1.2.1.4.21.1.5":  plain,
        ".1.3.6.1.2.1.4.21.1.6":  plain,
        ".1.3.6.1.2.1.4.21.1.7":  plain,
        ".1.3.6.1.2.1.4.21.1.8":  plain,
        ".1.3.6.1.2.1.4.21.1.9":  plain,
        ".1.3.6.1.2.1.4.21.1.10": plain,
        ".1.3.6.1.2.1.4.21.1.11": plain,
        ".1.3.6.1.2.1.4.21.1.12": plain,
        ".1.3.6.1.2.1.4.21.1.13": plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.1":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.2":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.3":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.4":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.5":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.6":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.7":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.8":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.9":  plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.10": plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.11": hex2ip,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.12": plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.13": hex2ip,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.14": plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.15": plain,
        ".1.3.6.1.4.1.9.9.449.1.3.1.1.16": plain
}

params = os.path.basename(args.file).rstrip(".txt").split("-")
ip_src = params[1]
timestamp = params[2]

file = open(args.file, "r")

for line in file:
	line = line.split(" ")
	value = ' '.join(line[1:]).strip()
	value = value.strip('\"')
	line = line[0]

	# parse ip route oid
	if line.startswith(".1.3.6.1.2.1.4.21.1"):
		line = line.split(".")
        	oid = '.'.join(line[0:11])
        	ip = '.'.join(line[11:15])

	# parse eigrp oid
	elif line.startswith(".1.3.6.1.4.1.9.9.449.1.3.1.1"):
		line = line.split(".")
		oid = '.'.join(line[0:15])
		ip = '.'.join(line[19:23])
		collection.update(
                        { "ip_src": ip_src, "timestamp": timestamp, "ip_dst": ip },
                        { "$set": { "cEigrpRouteMask" : int2netmask(line[23]), "cEigrpRouteMask2": line[23] } },
                        True
                )
		
	if oid in oid2name:
		collection.update(
			{ "ip_src": ip_src, "timestamp": timestamp, "ip_dst": ip },
			{ "$set": { oid2name[oid] : oid2parsefct[oid](value) } },
			True
		)			
		


