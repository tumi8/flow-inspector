# -*- coding: utf-8 -*-
# Copy this file to config.py and edit configs

# Server
#----------------------------------------------------------------
host = "0.0.0.0"
port = 8080
debug = True

# Original Flow DB (mysql, postrgres, oracle)
#----------------------------------------------------------------
flowDBHost="127.0.0.1"
flowDBPort=3306
flowDBUser="username"
flowDBPassword="password"
flowDBName="flows"


# Destination Flow Backend (Default: MongoDB)
#----------------------------------------------------------------
db_backend = "mongo"
db_host = "127.0.0.1"
db_port = 27017
db_user = None
db_password = None
db_name = "flows"

# Destination Data Backend (for miscelanous data)
#----------------------------------------------------------------
data_backend = "none"
data_backend_host = "127.0.0.1"
data_backend_port = 3306
data_backend_user = "username"
data_backend_password = "password"
data_backend_database = "flowinspector_data"
data_backend_snmp_table = "snmp"
data_backend_caching_threshold = 1000000


# Flow settings
#----------------------------------------------------------------
# The different bucket sizes in seconds to aggregate.
# Each bucket size leads to a new collection in the database.
# This list is assumed be sorted ascending!
flow_bucket_sizes = [ 10*60 ]
# Those values have to match in order to aggregate two flows
flow_aggr_values = ["sourceIPv4Address", "destinationIPv4Address", "sourceTransportPort", "destinationTransportPort", "protocolIdentifier"]
# Those columns will be summed up
flow_aggr_sums = ["packetDeltaCount", "octetDeltaCount"]
# Special treatment for ports:
# Only consider known port numbers, set the others to null
# before aggregation.
flow_filter_unknown_ports = False

# Preprocessor settings
#----------------------------------------------------------------
# caching can reduce the amount of writes to Mongo
# cache size per bucket size
pre_cache_size = 10000
# cache size for aggregated collections per bucket size
pre_cache_size_aggr = 5
# Defines whether this is a live or offline data import.
# The preprocessor will periodically try to flush its 
# cache in case of a live import. 
live_import=True



# Cleanup process settings
#----------------------------------------------------------------
# Important: preprocessor will not import any flows that are older
# (as in firstSwitched) than the default keep time if this value is 
# non zero.
max_flow_age = 0
 
# Host Information Datbase (optional)
#----------------------------------------------------------------
host_information_table = "somename"

# Destination Data Backend (for miscelanous data)
#----------------------------------------------------------------
host_info = "oracle"
host_info_host = "127.0.0.1"
host_info_port = 3306
host_info__user = "username"
host_info_password = "password"
host_info_name = "hostinfo"

# SNMP specfic options
#----------------------------------------------------------------
snmp_oid_file="flow-inspector/config/oid_list.txt"
snmp_query_tmp_dir="flow-inspector/tmp/snmp_query_cache/"
rrd_file_dir="flow-inspector/rrd/"

# Mail Reports
# --------------------------------------------------------------
smtp_host = "localhost"
smtp_port = 25
smtp_username = None
smtp_password = None
smtp_from = "vermont"
smtp_to   = [ "test@example.com" ]

