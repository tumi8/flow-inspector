#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os.path
import argparse
import time
import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))

import config
import common
import backend

from common_functions import *

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
	{"name": "ipAdEntAddr", "fct": ip2int},
	".1.3.6.1.2.1.4.20.1.2":
	{"name": "ipAdEntIfIndex", "fct": plain},
	".1.3.6.1.2.1.4.20.1.3":
	{"name": "ipAdEntNetMask", "fct": netmask2int},
	".1.3.6.1.2.1.4.20.1.4":
	{"name": "ipAdEntBcastAddr", "fct": plain},
	".1.3.6.1.2.1.4.20.1.5":
	{"name": "ipAdEntReasmMaxSize", "fct": plain},
	".1.3.6.1.2.1.4.21.1.1":
	{"name": "ipRouteDest", "fct": ip2int},
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
	{"name": "ipRouteNextHop", "fct": ip2int},
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
	{"name": "cEigrpDestNet", "fct": ip2int},
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
	{"name": "cEigrpRouteOriginAddr", "fct": hex2ip2int},
	".1.3.6.1.4.1.9.9.449.1.3.1.1.12":
	{"name": "cEigrpNextHopAddressType", "fct": plain},
	".1.3.6.1.4.1.9.9.449.1.3.1.1.13":
	{"name": "cEigrpNextHopAddress", "fct": hex2ip2int},
	".1.3.6.1.4.1.9.9.449.1.3.1.1.14":
	{"name": "cEigrpNextHopInterface", "fct": plain},
	".1.3.6.1.4.1.9.9.449.1.3.1.1.15":
	{"name": "cEigrpDistance", "fct": plain},
	".1.3.6.1.4.1.9.9.449.1.3.1.1.16":
	{"name": "cEigrpReportDistance", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.1":
	{"name": "ipCidrRouteDest", "fct": ip2int},
	".1.3.6.1.2.1.4.24.4.1.2":
	{"name": "ipCidrRouteMask", "fct": netmask2int},
	".1.3.6.1.2.1.4.24.4.1.3":
	{"name": "ipCidrRouteTos", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.4":
	{"name": "ipCidrRouteNextHop", "fct": ip2int},
	".1.3.6.1.2.1.4.24.4.1.5":
	{"name": "ipCidrRouteIfIndex", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.6":
	{"name": "ipCidrRouteType", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.7":
	{"name": "ipCidrRouteProto", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.8":
	{"name": "ipCidrRouteAge", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.9":
	{"name": "ipCidrRouteInfo", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.10":
	{"name": "ipCidrRouteNextHopAS", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.11":
	{"name": "ipCidrRouteMetric1", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.12":
	{"name": "ipCidrRouteMetric2", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.13":
	{"name": "ipCidrRouteMetric3", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.14":
	{"name": "ipCidrRouteMetric4", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.15":
	{"name": "ipCidrRouteMetric5", "fct": plain},
	".1.3.6.1.2.1.4.24.4.1.16":
	{"name": "ipCidrRouteStatus", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.1":
	{"name": "ifName", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.2":
	{"name": "ifInMulticastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.3":
	{"name": "ifInBroadcastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.4":
	{"name": "ifOutMulticastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.5":
	{"name": "ifOutBroadcastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.6":
	{"name": "ifHCInOctets", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.7":
	{"name": "ifHCInUcastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.8":
	{"name": "ifHCInMulticastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.9":
	{"name": "ifHCInBroadcastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.10":
	{"name": "ifHCOutOctets", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.11":
	{"name": "ifHCOutUcastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.12":
	{"name": "ifHCOutMulticastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.13":
	{"name": "ifHCOutBroadcastPkts", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.14":
	{"name": "ifLinkUpDownTrapEnable", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.15":
	{"name": "ifHighSpeed", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.16":
	{"name": "ifPromiscuousMode", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.17":
	{"name": "ifConnectorPresent", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.18":
	{"name": "ifAlias", "fct": plain},
	".1.3.6.1.2.1.31.1.1.1.19":
	{"name": "ifCounterDiscontinuityTime", "fct": plain},
	".1.3.6.1.4.1.9.9.368.1.15.2.1.1":
	{"name": "cssLoadBalancerSessionName", "fct": plain},
	".1.3.6.1.4.1.9.9.368.1.15.2.1.20":
	{"name": "cssLoadBalancerSessionCount", "fct": plain},
	".1.3.6.1.4.1.6213.2.4.2.5.1.2":
	{"name": "juniperClusterName", "fct": plain},
	".1.3.6.1.4.1.6213.2.4.2.1.1.2":
	{"name": "juniperClusterSessionCount", "fct": plain}
}

# dictionary containing table descriptions
fieldDict = {
	"interface_phy": {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"router": ("VARCHAR(15)", None, None),
		"if_number": ("INT UNSIGNED", None, None),
		"ifIndex": ("INT UNSIGNED", None, None),
		"ifDescr": ("VARCHAR(50)", None, None),
		"ifType": ("TINYINT UNSIGNED", None, None),
		"ifMtu": ("SMALLINT UNSIGNED", None, None),
		"ifSpeed": ("INT UNSIGNED", None, None),
		"ifPhysAddress": ("VARCHAR(20)", None, None),
		"ifAdminStatus": ("TINYINT UNSIGNED", None, None),
		"ifOperStatus": ("TINYINT UNSIGNED", None, None),
		"ifLastChange": ("VARCHAR(50)", None, None),
		"ifInOctets": ("INT UNSIGNED", None, None),
		"ifInUcastPkts": ("INT UNSIGNED", None, None),
		"ifInNUcastPkts": ("INT UNSIGNED", None, None),
		"ifInDiscards": ("INT UNSIGNED", None, None),
		"ifInErrors": ("INT UNSIGNED", None, None),
		"ifInUnknownProtos": ("INT UNSIGNED", None, None),
		"ifOutOctets": ("INT UNSIGNED", None, None),
		"ifOutUcastPkts": ("INT UNSIGNED", None, None),
		"ifOutNUcastPkts": ("INT UNSIGNED", None, None),
		"ifOutDiscards": ("INT UNSIGNED", None, None),
		"ifOutErrors": ("INT UNSIGNED", None, None),
		"ifOutQLen": ("INT UNSIGNED", None, None),
		"ifSpecific": ("VARCHAR(50)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "router ASC, if_number ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},

	"interface_log": {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"router": ("VARCHAR(15)", None, None),
		"if_ip": ("INT UNSIGNED", None, None),
		"ipAdEntAddr": ("INT UNSIGNED", None, None),
		"ipAdEntIfIndex": ("INT UNSIGNED", None, None),
		"ipAdEntNetMask": ("TINYINT UNSIGNED", None, None),
		"ipAdEntBcastAddr": ("BIT(1)", None, None),
		"ipAdEntReasmMaxSize": ("INT UNSIGNED", None, None),
		"index_preprocess": ("UNIQUE INDEX", "router ASC, if_ip ASC, timestamp ASC"),
		"index_findRoute": ("INDEX", "timestamp, ipAdEntAddr"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
		
	"ipRoute": {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"ip_src": ("INT UNSIGNED", None, None),
		"ip_dst": ("INT UNSIGNED", None, None),
		"low_ip": ("INT UNSIGNED", None, None),
		"high_ip": ("INT UNSIGNED", None, None),
		"ipRouteDest": ("INT UNSIGNED", None, None),
		"ipRouteIfIndex": ("INT UNSIGNED", None, None),
		"ipRouteMetric1": ("INT", None, None),
		"ipRouteMetric2": ("INT", None, None),
		"ipRouteMetric3": ("INT", None, None),
		"ipRouteMetric4": ("INT", None, None),
		"ipRouteMetric5": ("INT", None, None),
		"ipRouteNextHop": ("INT UNSIGNED", None, None),
		"ipRouteType": ("TINYINT UNSIGNED", None, None),
		"ipRouteProto": ("TINYINT UNSIGNED", None, None),
		"ipRouteAge": ("INT UNSIGNED", None, None),
		"ipRouteMask": ("TINYINT UNSIGNED", None, None),
		"ipRouteInfo": ("VARCHAR(50)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "ip_src ASC, ip_dst ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
		
	"cEigrp": {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"ip_src": ("INT UNSIGNED", None, None),
		"ip_dst": ("INT UNSIGNED", None, None),
		"mask_dst": ("TINYINT", None, None),
		"low_ip": ("INT UNSIGNED", None, None),
     	"high_ip": ("INT UNSIGNED", None, None),
		"cEigrpDestNetType": ("TINYINT UNSIGNED", None, None),
		"cEigrpDestNet": ("INT UNSIGNED", None, None),
		"cEigrpDestNetPrefixLen": ("TINYINT UNSIGNED", None, None),
		"cEigrpActive": ("TINYINT UNSIGNED", None, None),
		"cEigrpStuckInActive": ("TINYINT UNSIGNED", None, None),
		"cEigrpDestSuccessors": ("INT UNSIGNED", None, None),
		"cEigrpFdistance": ("INT UNSIGNED", None, None),
		"cEigrpRouteOriginType": ("VARCHAR(50)", None, None),
		"cEigrpRouteOriginAddrType": ("TINYINT UNSIGNED", None, None),
		"cEigrpRouteOriginAddr": ("INT UNSIGNED", None, None),
		"cEigrpNextHopAddressType": ("TINYINT UNSIGNED", None, None),
		"cEigrpNextHopAddress": ("INT UNSIGNED", None, None),
		"cEigrpNextHopInterface": ("VARCHAR(50)", None, None),
		"cEigrpDistance": ("INT UNSIGNED", None, None),
		"cEigrpReportDistance": ("INT UNSIGNED", None, None),
		"index_preprocess": ("UNIQUE INDEX", "ip_src ASC, ip_dst ASC, mask_dst ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
		
	"ipCidrRoute": {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"ip_src": ("INT UNSIGNED", None, None),
		"ip_dst": ("INT UNSIGNED", None, None),
		"mask_dst": ("TINYINT", None, None),
		"ip_gtw": ("INT UNSIGNED", None, None),
		"low_ip": ("INT UNSIGNED", None, None),
        "high_ip": ("INT UNSIGNED", None, None),
		"ipCidrRouteDest": ("INT UNSIGNED", None, None),
		"ipCidrRouteMask": ("TINYINT UNSIGNED", None, None),
		"ipCidrRouteTos": ("INT UNSIGNED", None, None),
		"ipCidrRouteNextHop": ("INT UNSIGNED", None, None),
		"ipCidrRouteIfIndex": ("INT UNSIGNED", None, None),
		"ipCidrRouteType": ("TINYINT UNSIGNED", None, None),
		"ipCidrRouteProto": ("TINYINT UNSIGNED", None, None),
		"ipCidrRouteAge": ("INT UNSIGNED", None, None),
		"ipCidrRouteInfo": ("VARCHAR(50)", None, None),
		"ipCidrRouteNextHopAS": ("INT UNSIGNED", None, None),
		"ipCidrRouteMetric1": ("INT", None, None),
		"ipCidrRouteMetric2": ("INT", None, None),
		"ipCidrRouteMetric3": ("INT", None, None),
		"ipCidrRouteMetric4": ("INT", None, None),
		"ipCidrRouteMetric5": ("INT", None, None),
		"ipCidrRouteStatus": ("TINYINT UNSIGNED", None, None),
		"index_preprocess": ("UNIQUE INDEX", "ip_src ASC, ip_dst ASC, ip_gtw ASC, mask_dst ASC, timestamp ASC"),
		"index_findRoute1": ("INDEX", "ipCidrRouteProto, timestamp, low_ip, high_ip"),
		"index_findRoute2": ("INDEX", "timestamp, ip_src, low_ip, high_ip"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},

	"ifXTable": {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"router": ("VARCHAR(15)", None, None),
		"if_number": ("INT UNSIGNED", None, None),
		"ifName": ("VARCHAR(50)", None, None),
		"ifInMulticastPkts": ("INT UNSIGNED", None, None),
		"ifInBroadcastPkts": ("INT UNSIGNED", None, None),
		"ifOutMulticastPkts": ("INT UNSIGNED", None, None),
		"ifOutBroadcastPkts": ("INT UNSIGNED", None, None),
		"ifHCInOctets": ("BIGINT UNSIGNED", None, None),
		"ifHCInUcastPkts": ("BIGINT UNSIGNED", None, None),
		"ifHCInMulticastPkts": ("BIGINT UNSIGNED", None, None),
		"ifHCInBroadcastPkts": ("BIGINT UNSIGNED", None, None),
		"ifHCOutOctets": ("BIGINT UNSIGNED", None, None),
		"ifHCOutUcastPkts": ("BIGINT UNSIGNED", None, None),
		"ifHCOutMulticastPkts": ("BIGINT UNSIGNED", None, None),
		"ifHCOutBroadcastPkts": ("BIGINT UNSIGNED", None, None),
		"ifLinkUpDownTrapEnable": ("TINYINT UNSIGNED", None, None),
		"ifHighSpeed": ("INT UNSIGNED", None, None),
		"ifPromiscuousMode": ("TINYINT UNSIGNED", None, None),
		"ifConnectorPresent": ("TINYINT UNSIGNED", None, None),
		"ifAlias": ("VARCHAR(70)", None, None),
		"ifCounterDiscontinuityTime": ("VARCHAR(50)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "router ASC, if_number ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},

	"juniperLoadbalancer" : {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"router": ("VARCHAR(15)", None, None),
		"session_number": ("INT UNSIGNED", None, None),
		"juniperClusterName": ("VARCHAR(150)", None, None),
		"juniperClusterSessionCount": ("BIGINT UNSIGNED", None, None),
	},
	"cssLoadbalancer" : {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("BIGINT UNSIGNED", None, None),
		"router": ("VARCHAR(15)", None, None),
		"scm_number": ("INT UNSIGNED", None, None),
		"ident" : ("VARCHAR(50)", None, None),
		"cssLoadBalancerSessionName": ("VARCHAR(50)", None, None),
		"cssLoadBalancerSessionCount": ("INT UNSIGNED", None, None)
	},

}

fieldDictOracle = {
	"interface_phy": {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(20)", None, None),
		"router": ("VARCHAR(15)", None, None),
		"if_number": ("NUMBER(11)", None, None),
		"ifIndex": ("NUMBER(11)", None, None),
		"ifDescr": ("VARCHAR(50)", None, None),
		"ifType": ("NUMBER(3)", None, None),
		"ifMtu": ("NUMBER(11)", None, None),
		"ifSpeed": ("NUMBER(11)", None, None),
		"ifPhysAddress": ("VARCHAR(20)", None, None),
		"ifAdminStatus": ("NUMBER(3)", None, None),
		"ifOperStatus": ("NUMBER(3)", None, None),
		"ifLastChange": ("VARCHAR(50)", None, None),
		"ifInOctets": ("NUMBER(11)", None, None),
		"ifInUcastPkts": ("NUMBER(11)", None, None),
		"ifInNUcastPkts": ("NUMBER(11)", None, None),
		"ifInDiscards": ("NUMBER(11)", None, None),
		"ifInErrors": ("NUMBER(11)", None, None),
		"ifInUnknownProtos": ("NUMBER(11)", None, None),
		"ifOutOctets": ("NUMBER(11)", None, None),
		"ifOutUcastPkts": ("NUMBER(11)", None, None),
		"ifOutNUcastPkts": ("NUMBER(11)", None, None),
		"ifOutDiscards": ("NUMBER(11)", None, None),
		"ifOutErrors": ("NUMBER(11)", None, None),
		"ifOutQLen": ("NUMBER(11)", None, None),
		"ifSpecific": ("VARCHAR(50)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "router ASC, if_number ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},

	"interface_log": {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(20)", None, None),
		"router": ("VARCHAR(15)", None, None),
		"if_ip": ("NUMBER(11)", None, None),
		"ipAdEntAddr": ("NUMBER(11)", None, None),
		"ipAdEntIfIndex": ("NUMBER(11)", None, None),
		"ipAdEntNetMask": ("NUMBER(3)", None, None),
		"ipAdEntBcastAddr": ("RAW(1)", None, None),
		"ipAdEntReasmMaxSize": ("NUMBER(11)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "router ASC, if_ip ASC, timestamp ASC"),
		"index_findRoute": ("INDEX", "timestamp, ipAdEntAddr"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
		
	"ipRoute": {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(20)", None, None),
		"ip_src": ("NUMBER(11)", None, None),
		"ip_dst": ("NUMBER(11)", None, None),
		"low_ip": ("NUMBER(11)", None, None),
		"high_ip": ("NUMBER(11)", None, None),
		"ipRouteDest": ("NUMBER(11)", None, None),
		"ipRouteIfIndex": ("NUMBER(11)", None, None),
		"ipRouteMetric1": ("NUMBER(11)", None, None),
		"ipRouteMetric2": ("NUMBER(11)", None, None),
		"ipRouteMetric3": ("NUMBER(11)", None, None),
		"ipRouteMetric4": ("NUMBER(11)", None, None),
		"ipRouteMetric5": ("NUMBER(11)", None, None),
		"ipRouteNextHop": ("NUMBER(11)", None, None),
		"ipRouteType": ("NUMBER(3)", None, None),
		"ipRouteProto": ("NUMBER(3)", None, None),
		"ipRouteAge": ("NUMBER(11)", None, None),
		"ipRouteMask": ("NUMBER(3)", None, None),
		"ipRouteInfo": ("VARCHAR(50)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "ip_src ASC, ip_dst ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
		
	"cEigrp": {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(20)", None, None),
		"ip_src": ("NUMBER(11)", None, None),
		"ip_dst": ("NUMBER(11)", None, None),
		"mask_dst": ("NUMBER(3)", None, None),
		"low_ip": ("NUMBER(11)", None, None),
     		"high_ip": ("NUMBER(11)", None, None),
		"cEigrpDestNetType": ("NUMBER(3)", None, None),
		"cEigrpDestNet": ("NUMBER(11)", None, None),
		"cEigrpDestNetPrefixLen": ("NUMBER(3)", None, None),
		"cEigrpActive": ("NUMBER(3)", None, None),
		"cEigrpStuckInActive": ("NUMBER(3)", None, None),
		"cEigrpDestSuccessors": ("NUMBER(11)", None, None),
		"cEigrpFdistance": ("NUMBER(11)", None, None),
		"cEigrpRouteOriginType": ("VARCHAR(50)", None, None),
		"cEigrpRouteOriginAddrType": ("NUMBER(3)", None, None),
		"cEigrpRouteOriginAddr": ("NUMBER(11)", None, None),
		"cEigrpNextHopAddressType": ("NUMBER(3)", None, None),
		"cEigrpNextHopAddress": ("NUMBER(11)", None, None),
		"cEigrpNextHopInterface": ("VARCHAR(50)", None, None),
		"cEigrpDistance": ("NUMBER(11)", None, None),
		"cEigrpReportDistance": ("NUMBER(11)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "ip_src ASC, ip_dst ASC, mask_dst ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
		
	"ipCidrRoute": {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(20)", None, None),
		"ip_src": ("NUMBER(11)", None, None),
		"ip_dst": ("NUMBER(11)", None, None),
		"mask_dst": ("NUMBER(3)", None, None),
		"ip_gtw": ("NUMBER(11)", None, None),
		"low_ip": ("NUMBER(11)", None, None),
	        "high_ip": ("NUMBER(11)", None, None),
		"ipCidrRouteDest": ("NUMBER(11)", None, None),
		"ipCidrRouteMask": ("NUMBER(3)", None, None),
		"ipCidrRouteTos": ("NUMBER(11)", None, None),
		"ipCidrRouteNextHop": ("NUMBER(11)", None, None),
		"ipCidrRouteIfIndex": ("NUMBER(11)", None, None),
		"ipCidrRouteType": ("NUMBER(3)", None, None),
		"ipCidrRouteProto": ("NUMBER(3)", None, None),
		"ipCidrRouteAge": ("NUMBER(11)", None, None),
		"ipCidrRouteInfo": ("VARCHAR(50)", None, None),
		"ipCidrRouteNextHopAS": ("NUMBER(11)", None, None),
		"ipCidrRouteMetric1": ("NUMBER(11)", None, None),
		"ipCidrRouteMetric2": ("NUMBER(11)", None, None),
		"ipCidrRouteMetric3": ("NUMBER(11)", None, None),
		"ipCidrRouteMetric4": ("NUMBER(11)", None, None),
		"ipCidrRouteMetric5": ("NUMBER(11)", None, None),
		"ipCidrRouteStatus": ("NUMBER(3)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "ip_src ASC, ip_dst ASC, ip_gtw ASC, mask_dst ASC, timestamp ASC"),
		"index_findRoute1": ("INDEX", "ipCidrRouteProto, timestamp, low_ip, high_ip"),
		"index_findRoute2": ("INDEX", "timestamp, ip_src, low_ip, high_ip"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},

	"ifXTable": {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(20)", None, None),
		"router": ("VARCHAR(15)", None, None),
		"if_number": ("NUMBER(11)", None, None),
		"ifName": ("VARCHAR(50)", None, None),
		"ifInMulticastPkts": ("NUMBER(11)", None, None),
		"ifInBroadcastPkts": ("NUMBER(11)", None, None),
		"ifOutMulticastPkts": ("NUMBER(11)", None, None),
		"ifOutBroadcastPkts": ("NUMBER(11)", None, None),
		"ifHCInOctets": ("NUMBER(20)", None, None),
		"ifHCInUcastPkts": ("NUMBER(20)", None, None),
		"ifHCInMulticastPkts": ("NUMBER(20)", None, None),
		"ifHCInBroadcastPkts": ("NUMBER(20)", None, None),
		"ifHCOutOctets": ("NUMBER(20)", None, None),
		"ifHCOutUcastPkts": ("NUMBER(20)", None, None),
		"ifHCOutMulticastPkts": ("NUMBER(20)", None, None),
		"ifHCOutBroadcastPkts": ("NUMBER(20)", None, None),
		"ifLinkUpDownTrapEnable": ("NUMBER(3)", None, None),
		"ifHighSpeed": ("NUMBER(11)", None, None),
		"ifPromiscuousMode": ("NUMBER(3)", None, None),
		"ifConnectorPresent": ("NUMBER(3)", None, None),
		"ifAlias": ("VARCHAR(70)", None, None),
		"ifCounterDiscontinuityTime": ("VARCHAR(50)", None, None),
		"index_preprocess": ("UNIQUE INDEX", "router ASC, if_number ASC, timestamp ASC"),
		"table_options": "ENGINE=MyISAM ROW_FORMAT=DYNAMIC"
	},
	"juniperLoadbalancer" : {
		"_id": ("NUMBER(20)", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(21)", None, None),
		"router": ("VARCHAR(15)", None, None),
		"session_number": ("NUMBER(11)", None, None),
		"juniperClusterName": ("VARCHAR(150)", None, None),
		"juniperClusterSessionCount": ("NUMBER(21)", None, None),
	},
	"cssLoadbalancer" : {
		"_id": ("BIGINT", "PRIMARY", "AUTO_INCREMENT"),
		"timestamp": ("NUMBER(21)", None, None),
		"router": ("VARCHAR(15)", None, None),
		"scm_number": ("NUMBER(11)", None, None),
		"ident" : ("VARCHAR(50)", None, None),
		"cssLoadBalancerSessionName": ("VARCHAR(50)", None, None),
		"cssLoadBalancerSessionCount": ("NUMBER(11)", None, None)
	},
}

def parse_snmp_file(file, doc):
	# parse file name
	params = os.path.basename(file).rstrip(".txt").split("-")
	source_type = params[0]
	ip_src = params[1]
	timestamp = params[2]
	lines = 0

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
					doc,
					"interface_phy",
					ip_src + '-' + interface + '-' + timestamp,
					{"router": ip_src, "if_number": interface,
						"timestamp": timestamp},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse interface_log oid
		elif line.startswith(".1.3.6.1.2.1.4.20.1"):
			line = line.split(".")
			oid = '.'.join(line[0:11])
			ip = '.'.join(line[11:15])

			if oid in oidmap:
				update_doc(
					doc,
					"interface_log",
					ip_src + '-' + ip + '-' + timestamp,
					{"router": ip_src, "if_ip": ip2int(ip),
						"timestamp": timestamp},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse ip route oid
		elif line.startswith(".1.3.6.1.2.1.4.21.1"):
			line = line.split(".")
			oid = '.'.join(line[0:11])
			ip = '.'.join(line[11:15])

			if oid in oidmap:
				update_doc(
					doc,
					"ipRoute",
					ip_src + '-' + ip + '-' + timestamp,
					{"ip_src": ip2int(ip_src), "timestamp": timestamp,
						"ip_dst": ip2int(ip)},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse eigrp oid
		elif line.startswith(".1.3.6.1.4.1.9.9.449.1.3.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			ip = '.'.join(line[19:23])

			if oid in oidmap:
				update_doc(
					doc,
					"cEigrp",
					ip_src + '-' + ip + '-' + line[23] + '-' + timestamp,
					{"ip_src": ip2int(ip_src), "timestamp": timestamp,
					"ip_dst": ip2int(ip), "mask_dst": line[23]},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse ipcidrroute oid
		elif line.startswith(".1.3.6.1.2.1.4.24.4.1"):
			line = line.split(".")
			oid = '.'.join(line[0:12])
			ip_dst = '.'.join(line[12:16])
			mask_dst = '.'.join(line[16:20])
			ip_gtw = '.'.join(line[21:25])

			if oid in oidmap:
				update_doc(
					doc,
					"ipCidrRoute",
					ip_src + '-' + ip_dst + '-' + mask_dst + '-' + ip_gtw + '-' + timestamp,
					{"ip_src": ip2int(ip_src), "timestamp": timestamp, "ip_dst": ip2int(ip_dst),
						"mask_dst": netmask2int(mask_dst), "ip_gtw": ip2int(ip_gtw)},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse ifxtable oid
		elif line.startswith(".1.3.6.1.2.1.31.1.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:12])
			if_number = line[12]

			if oid in oidmap:
				update_doc(
					doc,
					"ifXTable",
					ip_src + '-' + if_number + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "if_number": if_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse juniperLoadbalancer session name
		elif line.startswith(".1.3.6.1.4.1.6213.2.4.2.5.1.2"):
			line = line.split(".")
			oid = '.'.join(line[0:14])
			session_number = line[14]

			if oid in oidmap:
				update_doc(
					doc,
					"juniperLoadbalancer",
					ip_src + '-' + session_number + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "session_number": session_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse juniperLoadbalancer session counter
		elif line.startswith(".1.3.6.1.4.1.6213.2.4.2.1.1.2"):
			line = line.split(".")
			oid = '.'.join(line[0:14])
			session_number = line[14]

			if oid in oidmap:
				update_doc(
					doc,
					"juniperLoadbalancer",
					ip_src + '-' + session_number + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "session_number": session_number},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# parse cisco cluster name
		elif line.startswith(".1.3.6.1.4.1.9.9.368.1.15.2.1.1"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			scm_number = line[15]
			ident = '.'.join(line[16:])

			if oid in oidmap:
				update_doc(
					doc,
					"cssLoadbalancer",
					ip_src + '-' + scm_number + '-' + ident + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "scm_number": scm_number, "ident": ident},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# aprse cisco cluster session count
		elif line.startswith(".1.3.6.1.4.1.9.9.368.1.15.2.1.20"):
			line = line.split(".")
			oid = '.'.join(line[0:15])
			scm_number = line[15]
			ident = '.'.join(line[16:])

			if oid in oidmap:
				update_doc(
					doc,
					"cssLoadbalancer",
					ip_src + '-' + scm_number + '-' + ident + '-' + timestamp,
					{"router": ip_src, "timestamp": timestamp, "scm_number": scm_number, "ident": ident},
					{oidmap[oid]["name"]: oidmap[oid]["fct"](value)}
				)

		# increment counter for processed lines
		lines += 1

	return (lines, timestamp, doc)

def getFieldDict(args):
	if args.backend == "mysql":
		return fieldDict
	elif args.backend == "oracle":
		return fieldDictOracle
	elif args.backend == "mongo":
		return None
	else:
		raise Exception("Unknown data backend: " + args.backend);

def update_doc(doc, table, table_key, db_key, db_values):
	""" update local document before comitting to databackend """
	if not table in doc:
		doc[table] = dict()
	if table_key in doc[table]:
		doc[table][table_key][1]["$set"].update(db_values)
	else:
		doc[table][table_key] = (db_key, {"$set": db_values})


def commit_doc(doc, collections):
	time_begin = time.time()
	time_last = time_begin
	time_current = 0
	counter = 0
	total = sum(len(doc[table]) for table in doc)

	print "Commiting %s entries to databackend" % total
	
	for name, table in doc.items():
		# push into the database
		for value in table.itervalues():
			collections[name].update(value[0], value[1], True)
			counter = counter + 1
		time_current = time.time()
		if (time_current - time_last > 5):
			print "Processed {0} entries in {1} seconds ({2} entries per second, {3}% done)".format(
				counter, time_current - time_begin,
				counter / (time_current - time_begin), 100.0 * counter / total)
			time_last = time_current 
	doc = {}

def main():
	doc = {}
	collections = dict()


	parser = argparse.ArgumentParser(description="Parse SNMP data files and import data to database")
	parser.add_argument("data_path", help="Path to the data that should be inserted. This must be a file if neither -d or -r are given.")
	parser.add_argument("-d", "--directory", action="store_true", help="Parse directory instead of a single file. The directory will be scanned for <directory>/*.txt:")
	parser.add_argument("-r", "--recursive", action="store_true", help="Recurse direcory, i.e. expecting files in <directory>/*/*.txt")
	parser.add_argument("--dst-host", nargs="?", default=config.data_backend_host, help="Backend database host")
	parser.add_argument("--dst-port", nargs="?", default=config.data_backend_port, type=int, help="Backend database port")
	parser.add_argument("--dst-user", nargs="?", default=config.data_backend_user, help="Backend database user")
	parser.add_argument("--dst-password", nargs="?", default=config.data_backend_password, help="Backend database password")
	parser.add_argument("--dst-database", nargs="?", default=config.data_backend_snmp_name, help="Backend database name")
	parser.add_argument("--clear-database", nargs="?", type=bool, default=False, const=True, help="Whether to clear the whole databse before importing any flows.")
	parser.add_argument( "--backend", nargs="?", default=config.data_backend, const=True, help="Selects the backend type that is used to store the data")


	args = parser.parse_args()

	dst_db = backend.databackend.getBackendObject(
		args.backend, args.dst_host, args.dst_port,
		args.dst_user, args.dst_password, args.dst_database)

	if args.clear_database:
		dst_db.clearDatabase()

	for name, fields in getFieldDict(args).items():
		dst_db.prepareCollection(name, fields)
		collections[name] = dst_db.getCollection(name)
	
	# TODO: hacky ... make something more general ...
	if backend == "mongo":
		db = pymongo.Connection(args.dst_host, args.dst_port)[args.dst_database]
		collection = db["snmp_raw"]
		collection.ensure_index([("router", pymongo.ASCENDING), ("if_number", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])
		collection.ensure_index([("router", pymongo.ASCENDING), ("if_ip", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])
		collection.ensure_index([("ip_src", pymongo.ASCENDING), ("ip_dst", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])
		collection.ensure_index([("ip_src", pymongo.ASCENDING), ("ip_dst", pymongo.ASCENDING), ("mask_dst", pymongo.ASCENDING), ("ip_gtw", pymongo.ASCENDING), ("timestamp", pymongo.ASCENDING), ("type", pymongo.ASCENDING)])

		# restore generic backend collection
		collection = dst_db.getCollection("snmp_raw")
	else: 
	#	collection.createIndex("router")
	#	collection.createIndex("if_number")
	#	collection.createIndex("timestamp")
	#	collection.createIndex("type")
	#	collection.createIndex("ip_src")
	#	collection.createIndex("ip_dst")
		pass

	# enviromental settings
	cache_treshold = 10000000

	# statistical counters
	time_begin = time.time()
	time_last = time_begin
	counter = 0

	# local document storage
	lines_since_commit = 0
	timestamps = set()

	
	# TODO: implies precedence of operators, maybe something better can be done here
	if args.directory:
		files = glob.glob(args.data_path + "/*.txt")
	elif args.recursive:
		files = glob.glob(args.data_path + "/*/*.txt")
	else:
		files = [ args.data_path ]
	
	for file in files:
			(read_lines, timestamp, doc) = parse_snmp_file(file, doc)
			lines_since_commit += read_lines
			counter += read_lines
			timestamps.add(timestamp)
			if lines_since_commit > cache_treshold:
				commit_doc(doc, collections)
				lines_since_commit = 0

			# do statistical calculation
			time_current = time.time()
			if (time_current - time_last > 5):
				print "Processed %s lines in %s seconds (%s lines per second)" % (
					counter, time_current - time_begin, counter / (time_current - time_begin))
				time_last = time_current

	
	#	print "counter: %s" % counter


	# commit local doc to databackend in the end
	
	commit_doc(doc, collections)

	for collection in collections.itervalues():
		collection.flushCache()

	print "Calculating IP ranges"

	# calculate ip network ranges
	for timestamp in timestamps:
		for row in collections["ipCidrRoute"].find({"timestamp": timestamp}):
			(low_ip, high_ip) = calc_ip_range(row["ip_dst"], row["mask_dst"])
			collections["ipCidrRoute"].update({"_id": row["_id"]}, {"$set": {"low_ip": low_ip, "high_ip": high_ip}}, True)
	
		for row in collections["cEigrp"].find({"timestamp": timestamp}):
			(low_ip, high_ip) = calc_ip_range(row["ip_dst"], int(row["mask_dst"]))
			collections["cEigrp"].update({"_id": row["_id"]}, {"$set": {"low_ip": low_ip, "high_ip": high_ip}}, True)

	for collection in collections.itervalues():
		collection.flushCache()

	# do some statistics in the end
	time_current = time.time()
	print "Processed %s lines in %s seconds (%s lines per second)" % (
			counter, time_current - time_begin, counter / (time_current - time_begin))

if __name__ == "__main__":
	main()
