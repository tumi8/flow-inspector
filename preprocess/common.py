import os 


# flow time interval column names
COL_FIRST_SWITCHED = "firstSwitched"
COL_LAST_SWITCHED = "lastSwitched"
# column names of IP addresses
COL_SRC_IP = "srcIP"
COL_DST_IP = "dstIP"
# column names of ports and protocol
COL_SRC_PORT = "srcPort"
COL_DST_PORT = "dstPort"
COL_PROTO = "proto"

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
	
