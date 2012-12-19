#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
import argparse
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
import common
import backend
import config

parser = argparse.ArgumentParser(description="Preprocess SNMP data")
parser.add_argument(
    "file", nargs="*", help="File to parse")
parser.add_argument(
    "--dst-host", nargs="?", default=config.db_host,
    help="Backend database host")
parser.add_argument(
    "--dst-port", nargs="?", default=config.db_port,
    type=int, help="Backend database port")
parser.add_argument(
    "--dst-user", nargs="?", default=config.db_user,
    help="Backend database user")
parser.add_argument(
    "--dst-password", nargs="?",
    default=config.db_password, help="Backend database password")
parser.add_argument(
    "--dst-database", nargs="?",
    default=config.db_name, help="Backend database name")
parser.add_argument(
    "--clear-database", nargs="?", type=bool, default=False, const=True,
    help="Whether to clear the whole databse before importing any flows.")
parser.add_argument(
    "--backend", nargs="?", default=config.db_backend, const=True,
    help="Selects the backend type that is used to store the data")

args = parser.parse_args()

dst_db = backend.flowbackend.getBackendObject(
    args.backend, args.dst_host, args.dst_port,
    args.dst_user, args.dst_password, args.dst_database)

if args.clear_database:
    dst_db.clearDatabase()

dst_db.prepareCollections()
collection = dst_db.getCollection("snmp_raw")


# parsing function for oid values

def plain(value):
    """ do nothing function """
    return value


def hex2ip(value):
    """ convert hex to ip (i.e. DE AD BE EF -> 222.173.190.239) """
    value = value.strip(" ")
    return '.'.join(str(int(part, 16))
           for part in [value[0:2], value[3:5], value[6:8], value[9:11]])


def netmask2int(netmask):
    """ convert netmask to int (i.e. 255.255.255.0 -> 24) """
    tmp = ''
    for part in netmask.split("."):
        tmp = tmp + str(bin(int(part)))
    return tmp.count("1")


def int2netmask(value):
    """ convert int to netmask (i.e. 24 -> 255.255.255.0) """
    value = '1' * int(value) + '0' * (32 - int(value))
    return '.'.join(str(int(part, 2))
           for part in [value[0:8], value[8:16], value[16:24], value[24:32]])


def ip2int(ip):
    """ convert ip to int """
    ip = ip.split('.')
    return (int(ip[0]) * (2 ** 24) + int(ip[1]) * (2 ** 16) +
            int(ip[2]) * (2 ** 8) + int(ip[3]))


def int2ip(i):
    """ convert int to ip """
    return (str(i // (2 ** 24)) + "." + str((i // (2 ** 16)) % 256) + "." +
            str((i // (2 ** 8)) % 256) + "." + str(i % 256))


# dictionary which maps oid -> name and fct to parse oid value
oidmap = {
    ".1.3.6.1.2.1.2.2.1.1":
    {"name": "ifIndex", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.2":
    {"name": "ifDescr", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.3":
    {"name": "ifType", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.4":
    {"name": "ifMtu", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.5":
    {"name": "ifSpeed", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.6":
    {"name": "ifPhysAddress", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.7":
    {"name": "ifAdminStatus", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.8":
    {"name": "ifOperStatus", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.9":
    {"name": "ifLastChange", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.10":
    {"name": "ifInOctets", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.11":
    {"name": "ifInUcastPkts", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.12":
    {"name": "ifInNUcastPkts", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.13":
    {"name": "ifInDiscards", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.14":
    {"name": "ifInErrors", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.15":
    {"name": "ifInUnknownProtos", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.16":
    {"name": "ifOutOctets", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.17":
    {"name": "ifOutUcastPkts", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.18":
    {"name": "ifOutNUcastPkts", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.19":
    {"name": "ifOutDiscards", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.20":
    {"name": "ifOutErrors", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.21":
    {"name": "ifOutQLen", "fct": plain},
    ".1.3.6.1.2.1.2.2.1.22":
    {"name": "ifSpecific", "fct": plain},
    ".1.3.6.1.2.1.4.20.1.1":
    {"name": "ipAdEntAddr", "fct": plain},
    ".1.3.6.1.2.1.4.20.1.2":
    {"name": "ipAdEntIfIndex", "fct": plain},
    ".1.3.6.1.2.1.4.20.1.3":
    {"name": "ipAdEntNetMask", "fct": netmask2int},
    ".1.3.6.1.2.1.4.20.1.4":
    {"name": "ipAdEntBcastAddr", "fct": plain},
    ".1.3.6.1.2.1.4.20.1.5":
    {"name": "ipAdEntReasmMaxSize", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.1":
    {"name": "ipRouteDest", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.2":
    {"name": "ipRouteIfIndex", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.3":
    {"name": "ipRouteMetric1", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.4":
    {"name": "ipRouteMetric2", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.5":
    {"name": "ipRouteMetric3", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.6":
    {"name": "ipRouteMetric4", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.7":
    {"name": "ipRouteNextHop", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.8":
    {"name": "ipRouteType", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.9":
    {"name": "ipRouteProto", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.10":
    {"name": "ipRouteAge", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.11":
    {"name": "ipRouteMask", "fct": netmask2int},
    ".1.3.6.1.2.1.4.21.1.12":
    {"name": "ipRouteMetric5", "fct": plain},
    ".1.3.6.1.2.1.4.21.1.13":
    {"name": "ipRouteInfo", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.1":
    {"name": "cEigrpDestNetType", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.2":
    {"name": "cEigrpDestNet", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.3":
    {"name": "cEigrpDestNetPrefixLen", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.4":
    {"name": "cEigrpDestNetPrefixLen", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.5":
    {"name": "cEigrpActive", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.6":
    {"name": "cEigrpStuckInActive", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.7":
    {"name": "cEigrpDestSuccessors", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.8":
    {"name": "cEigrpFdistance", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.9":
    {"name": "cEigrpRouteOriginType", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.10":
    {"name": "cEigrpRouteOriginAddrType", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.11":
    {"name": "cEigrpRouteOriginAddr", "fct": hex2ip},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.12":
    {"name": "cEigrpNextHopAddressType", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.13":
    {"name": "cEigrpNextHopAddress", "fct": hex2ip},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.14":
    {"name": "cEigrpNextHopInterface", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.15":
    {"name": "cEigrpDistance", "fct": plain},
    ".1.3.6.1.4.1.9.9.449.1.3.1.1.16":
    {"name": "cEigrpReportDistance", "fct": plain}
}


def update_doc(doc_key, mongo_key, mongo_values):
    """ update local document before comitting to mongo db """
    if doc_key in doc:
        doc[doc_key][1]["$set"].update(mongo_values)
    else:
        doc[doc_key] = (mongo_key, {"$set": mongo_values})

# statistical counters
begin = time.time()
last = begin
counter = 0

for file in args.file:

    # local document storage
    doc = {}

    # parse file name
    params = os.path.basename(file).rstrip(".txt").split("-")
    source_type = params[0]
    ip_src = params[1]
    timestamp = params[2]

    # read and process file contents
    file = open(file, "r")
    for line in file:
        index = line.find(" ")
        value = line[index + 1:]
        value = value.strip("\n")
        value = value.strip('"')
        line = line[0:index]

        # parse interface_phy oid
        if line.startswith(".1.3.6.1.2.1.2.2.1"):
            line = line.split(".")
            oid = '.'.join(line[0:11])
            interface = line[11]

            if oid in oidmap:
                update_doc(
                    ip_src + "int" + interface,
                    {"router": ip_src, "if_number": interface,
                        "timestamp": timestamp, "type": "interface_phy"},
                    {oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
                )

        # parse interface_log oid
        elif line.startswith(".1.3.6.1.2.1.4.20.1"):
            line = line.split(".")
            oid = '.'.join(line[0:11])
            ip = '.'.join(line[11:15])

            if oid in oidmap:
                update_doc(
                    ip_src + "_ip_" + ip,
                    {"router": ip_src, "if_ip": ip,
                        "timestamp": timestamp, "type": "interface_log"},
                    {oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
                )

        # parse ip route oid
        elif line.startswith(".1.3.6.1.2.1.4.21.1"):
            line = line.split(".")
            oid = '.'.join(line[0:11])
            ip = '.'.join(line[11:15])

            if oid in oidmap:
                update_doc(
                    ip_src + ip,
                    {"ip_src": ip_src, "timestamp": timestamp,
                        "ip_dst": ip, "type": "route"},
                    {oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
                )

        # parse eigrp oid
        elif line.startswith(".1.3.6.1.4.1.9.9.449.1.3.1.1"):
            line = line.split(".")
            oid = '.'.join(line[0:15])
            ip = '.'.join(line[19:23])

            if oid in oidmap:
                update_doc(
                    ip_src + ip,
                    {"ip_src": ip_src, "timestamp": timestamp,
                        "ip_dst": ip, "type": "eigrp"},
                    {"cEigrpRouteMask": line[23],
                        oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
                )

        # incremt counter for processed lines
        counter = counter + 1

        # do statistical calculation
        current = time.time()
        if (current - last > 5):
            print "Processed {0} lines in {1} seconds ({2} lines per second)".format(
                str(counter),
                (current - begin),
                counter / (current - begin))
            last = current

    # commit local doc to mongo db
    for value in doc.itervalues():
        collection.update(value[0], value[1], True)

# do some statistics in the end
current = time.time()
print "Processed {0} lines in {1} seconds ({2} lines per second)".format(
    str(counter),
    (current - begin),
    counter / (current - begin)
)
