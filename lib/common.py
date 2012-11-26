import os 


# flow time interval column names
COL_FIRST_SWITCHED = "flowStartSeconds"
COL_LAST_SWITCHED = "flowEndSeconds"
# column names of IP addresses
COL_SRC_IP = "sourceIPv4Address"
COL_DST_IP = "destinationIPv4Address"

COL_IPADDRESS = "ipaddress"
COL_PORT = "port"

# column names of ports and protocol
COL_SRC_PORT = "sourceTransportPort"
COL_DST_PORT = "destinationTransportPort"
COL_PROTO = "protocolIdentifier"
COL_BUCKET = "bucket"


COL_BYTES = "octetDeltaCount"
COL_PKTS = "packetDeltaCount"
COL_FLOWS = "flows"
COL_ID = "id"

COL_PROTO_TCP = "tcp"
COL_PROTO_UDP = "udp"
COL_PROTO_ICMP = "icmp"
COL_PROTO_OTHER = "other"
AVAILABLE_PROTOS = [ COL_PROTO_TCP, COL_PROTO_UDP, COL_PROTO_ICMP, COL_PROTO_OTHER ]


# the collection prefix to use for flows
DB_FLOW_PREFIX = "flows_"
# the collection prefix to use for completely aggregated flows
DB_FLOW_AGGR_PREFIX = "flows_aggr_"
# the collection to use for the node index
DB_INDEX_NODES = "index_nodes"
# the collection to use for the port index
DB_INDEX_PORTS = "index_ports"

IGNORE_COLUMNS = ["firstSwitchedMillis", "lastSwitchedMillis"]

# Print output every ... in seconds
OUTPUT_INTERVAL = 10

# the xml file containing known port numbers
PORTS_FILE = os.path.join(os.path.dirname(__file__), '..', 'config', 'service-names-port-numbers.xml')

REDIS_QUEUE_KEY = "entry:queue"

HOST_INFORMATION_COLLECTION = "HOST_INFORMATION_CHECKER"

# Oracle is a professional environment. We therefore need to perform
# some special mappings for table column names: Oracle is case sensitive
# and does not cope with anything that is not upper case *sigh*
LEGACY_COLUMNMAP = {
	"ID"	        : COL_ID,
	"FLOWS"         : COL_FLOWS,
        "SRCIP"         : COL_SRC_IP,
        "DSTIP"         : COL_DST_IP,
        "SRCPORT"       : COL_SRC_PORT,
        "DSTPORT"       : COL_DST_PORT,
        "PROTO"         : COL_PROTO,
        "BYTES"         : COL_BYTES,
        "PKTS"          : COL_PKTS,
        "FIRSTSWITCHED" : COL_FIRST_SWITCHED,
        "LASTSWITCHED"  : COL_LAST_SWITCHED,
}

ORACLE_COLUMNMAP = {
	COL_ID.upper()             : COL_ID,
	COL_BUCKET.upper()         : COL_BUCKET,
	COL_FLOWS.upper()          : COL_FLOWS,
	COL_SRC_IP.upper()         : COL_SRC_IP,
	COL_DST_IP.upper()         : COL_DST_IP,
	COL_SRC_PORT.upper()       : COL_SRC_PORT,
	COL_DST_PORT.upper()       : COL_DST_PORT,
	COL_PROTO.upper()          : COL_PROTO,
	COL_BYTES.upper()          : COL_BYTES,
	COL_PKTS.upper()           : COL_PKTS,
	COL_FIRST_SWITCHED.upper() : COL_FIRST_SWITCHED,
	COL_LAST_SWITCHED.upper()  : COL_LAST_SWITCHED,
	"SRC"                      : "src",
	"DST"                      : "dst",
	COL_PROTO_TCP.upper()      : COL_PROTO_TCP,
	COL_PROTO_UDP.upper()      : COL_PROTO_UDP,
	COL_PROTO_ICMP.upper()     : COL_PROTO_ICMP,
	COL_PROTO_OTHER.upper()    : COL_PROTO_OTHER
	
}

MYSQL_TYPE_MAPPER = {
	COL_ID		   : "INTEGER(20) UNSIGNED",
	COL_BUCKET	   : "INTEGER(10) UNSIGNED",
	COL_SRC_IP         : "INTEGER(10) UNSIGNED",
	COL_DST_IP         : "INTEGER(10) UNSIGNED",
	COL_SRC_PORT       : "SMALLINT(5) UNSIGNED",
	COL_DST_PORT       : "SMALLINT(5) UNSIGNED",
	COL_PROTO          : "TINYINT(3) UNSIGNED",
	COL_FLOWS          : "BIGINT(20) UNSIGNED",
	COL_BYTES          : "BIGINT(20) UNSIGNED",
	COL_PKTS           : "BIGINT(20) UNSIGNED",
	COL_FIRST_SWITCHED : "INTEGER(10) UNSIGNED",
	COL_LAST_SWITCHED  : "INTEGER(10) UNSIGNED"
}

ORACLE_TYPE_MAPPER = {
	COL_SRC_IP         : "NUMBER(10)",
	COL_DST_IP         : "NUMBER(10)",
	COL_SRC_PORT       : "NUMBER(5)",
	COL_DST_PORT       : "NUMBER(5)",
	COL_PROTO          : "NUMBER(3)",
	COL_FLOWS          : "NUMBER(20)",
	COL_BYTES          : "NUMBER(20)",
	COL_PKTS           : "NUMBER(20)",
	COL_FIRST_SWITCHED : "NUMBER(10)",
	COL_LAST_SWITCHED  : "NUMBER(10)"
}

def getProto(obj):
	return getProtoFromValue(obj.get(COL_PROTO, 0))

def getProtoFromValue(proto):
	if proto == 17 or proto == "UDP":
		result = COL_PROTO_UDP
	elif proto == 6 or proto == "TCP":
		result = COL_PROTO_TCP
	elif proto == 1 or proto == "ICMP":
		result = COL_PROTO_ICMP
	else:
		result = COL_PROTO_OTHER

	return result

def getValueFromProto(proto):
	if proto == COL_PROTO_UDP:
		return 17
	if proto == COL_PROTO_TCP:
		return 6
	if proto == COL_PROTO_ICMP:
		return 1
	return 0
	
# read ports for special filtering
def getKnownPorts(flow_filter_unknown_ports):
	known_ports = None
	if flow_filter_unknown_ports:
		f = open(PORTS_FILE, "r")
		dom = xml.dom.minidom.parse(f)
		f.close()
	
		def getDomText(node):
			rc = []
			for n in node.childNodes:
				if n.nodeType == node.TEXT_NODE:
					rc.append(n.data)
			return ''.join(rc)

		known_ports = dict()
		records = dom.getElementsByTagName("record")
		for record in records:
			description = getDomText(record.getElementsByTagName("description")[0])
			number = record.getElementsByTagName("number")
			if description != "Unassigned" and len(number) > 0:
				numbers = getDomText(number[0]).split('-')
				number = int(numbers[0])
				number_to = int(numbers[len(numbers)-1])
				
				protocol = record.getElementsByTagName("protocol")
				if len(protocol) > 0:
					protocol = getDomText(protocol[0])
					if protocol == "tcp":
						protocol = 6
					elif protocol == "udp":
						protocol = 17
					else:
						protocol = 0
				else:
					protocol = 0
				
				while number <= number_to:
					if number in known_ports:
						known_ports[number].append(protocol)
					else:
						known_ports[number] = [protocol]
					number += 1


def update_node_index(obj, collection, aggr_sum):
	"""Update the node index collection in MongoDB with the current flow.
	
	:Parameters:
	 - `obj`: A dictionary containing a flow.
	 - `collection`: A pymongo collection to insert the documents.
	 - `aggr_sum`: A list of keys which will be sliced and summed up.
	"""

	# update source node
	doc = { "$inc": {} }
	

	proto = getProto(obj)

	for s in aggr_sum:
		doc["$inc"][s] = obj.get(s, 0)
		doc["$inc"][proto + "." + s] = obj.get(s,0)
		doc["$inc"]["src." + s] = obj.get(s, 0)
		doc["$inc"]["src." + proto + "." + s] = obj.get(s,0)

	if COL_FLOWS in obj:
		flows = obj[COL_FLOWS]
	else:
		flows = 1

	doc["$inc"][COL_FLOWS] = flows
	doc["$inc"][proto + "." + COL_FLOWS] = flows
	doc["$inc"]["src." + COL_FLOWS] = flows
	doc["$inc"]["src." + proto + "." + COL_FLOWS] = flows
	
	# insert if not exists, else update sums
	if COL_BUCKET in obj:
		collection.update({COL_ID: obj[COL_SRC_IP], COL_BUCKET: obj[COL_BUCKET] }, doc, True)
	else:
		collection.update({COL_ID: obj[COL_SRC_IP] }, doc, True)
	
	# update destination node
	doc = { "$inc": {} }
	
	for s in aggr_sum:
		doc["$inc"][s] = obj.get(s, 0)
		doc["$inc"][proto + "." + s] = obj.get(s,0)
		doc["$inc"]["dst." + s] = obj.get(s, 0)
		doc["$inc"]["dst." + proto + "." + s] = obj.get(s,0)

	if COL_FLOWS in obj:
		flows = obj[COL_FLOWS]
	else:
		flows = 1

	doc["$inc"][COL_FLOWS] = flows
	doc["$inc"][proto + "." + COL_FLOWS] = flows 
	doc["$inc"]["dst." + COL_FLOWS] = flows 
	doc["$inc"]["dst." + proto + "." + COL_FLOWS] = flows
					
	# insert if not exists, else update sums
	if COL_BUCKET in obj:
		collection.update({COL_ID: obj[COL_DST_IP], COL_BUCKET: obj[COL_BUCKET] }, doc, True)
	else:
		collection.update({COL_ID: obj[COL_DST_IP] }, doc, True)

	# update total counters
	doc = { "$inc": {} }

	if COL_FLOWS in obj:
		flows = obj[COL_FLOWS]
	else:
		flows = 1

	doc["$inc"][COL_FLOWS] = flows 
	doc["$inc"][proto + "." + COL_FLOWS]= flows 


	for s in aggr_sum:
		doc["$inc"][s] = obj.get(s, 0)
		doc["$inc"][proto + "." + s] = obj.get(s, 0)

	if COL_BUCKET in obj:
		collection.update({COL_ID:  "total", COL_BUCKET: obj[COL_BUCKET] }, doc, True)
	else:
		collection.update({COL_ID : "total"  }, doc, True)

	
def update_port_index(obj, collection, aggr_sum, filter_ports):
	"""Update the port index collection in MongoDB with the current flow.
	
	:Parameters:
	 - `obj`: A dictionary containing a flow.
	 - `collection`: A pymongo collection to insert the documents.
	 - `aggr_sum`: A list of keys which will be sliced and summed up.
	 - `filter_ports`: A dictionary of ports and protocols to remove unknown ports
	"""
	
	# update source port
	doc = { "$inc": {} }

	for s in aggr_sum:
		doc["$inc"][s] = obj.get(s, 0)
		doc["$inc"]["src." + s] = obj.get(s, 0)

	if COL_FLOWS in obj:
		flows = obj[COL_FLOWS]
	else:
		flows = 1

	doc["$inc"][COL_FLOWS] = flows
	doc["$inc"]["src." + COL_FLOWS] = flows

	# set unknown ports to None
	port = obj.get(COL_SRC_PORT, None)
	if filter_ports and port != None:
		if port in filter_ports:
			proto = int(obj.get(COL_PROTO, -1))
			if proto >= 0 and not proto in filter_ports[port]:
				port = None
		else:
			port = None
	
	# insert if not exists, else update sums
	if COL_BUCKET in obj:
		collection.update({ COL_ID: port, COL_BUCKET: obj[COL_BUCKET]}, doc, True)
	else:
		collection.update({ COL_ID: port }, doc, True)
	
	# update destination port
	doc = { "$inc": {} }

	for s in aggr_sum:
		doc["$inc"][s] = obj.get(s, 0)
		doc["$inc"]["dst." + s] = obj.get(s, 0)

	if COL_FLOWS in obj:
		flows = obj[COL_FLOWS]
	else:
		flows = 1
	
	doc["$inc"][COL_FLOWS] = flows
	doc["$inc"]["dst." + COL_FLOWS] = flows
	
	# insert if not exists, else update sums
	if COL_BUCKET in obj:
		collection.update({COL_ID:  port, COL_BUCKET: obj[COL_BUCKET] }, doc, True)
	else:
		collection.update({COL_ID: port}, doc, True)

	# update total counters
	doc = { "$inc": {} }

	if COL_FLOWS in obj:
		flows = obj[COL_FLOWS]
	else:
		flows = 1

	doc["$inc"][COL_FLOWS] = flows 
	for s in aggr_sum:
		doc["$inc"][s] = obj.get(s, 0)

	if COL_BUCKET in obj:
		collection.update({COL_ID : "total", COL_BUCKET: obj[COL_BUCKET]}, doc, True)
	else:
		collection.update({COL_ID : "total"}, doc, True)

