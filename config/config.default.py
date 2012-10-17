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
data_backend = "mysql"
data_backend_host = "127.0.0.1"
data_backend_port = 3306
data_backend_user = "username"
data_backend_password = "password"
data_backend_name = "flows"

# Destination Data Backend (for miscelanous data)
#----------------------------------------------------------------
host_info = "oracle"
data_backend_host = "127.0.0.1"
data_backend_port = 3306
data_backend_user = "username"
data_backend_password = "password"
data_backend_name = "hostinfo"


# Flow settings
#----------------------------------------------------------------
# The different bucket sizes in seconds to aggregate.
# Each bucket size leads to a new collection in the database.
# This list is assumed be sorted ascending!
flow_bucket_sizes = [60, 10*60, 60*60, 24*60*60]
# Those values have to match in order to aggregate two flows
flow_aggr_values = ["srcIP", "dstIP", "srcPort", "dstPort", "proto"]
# Those columns will be summed up
flow_aggr_sums = ["pkts", "bytes"]
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


# Cleanup process settings
#----------------------------------------------------------------
# Important: preprocessor will not import any flows that are older
# (as in firstSwitched) than the default keep time if this value is 
# non zero.
max_flow_age = 60*60*24*7
 

# PCAP processor settings
#----------------------------------------------------------------
pcap_output_dir = '/opt/data/pcap_output/'
gnuplot_path='/usr/bin/gnuplot'

