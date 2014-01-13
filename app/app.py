#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Flow Inspector - Visual Network Flow Analyis

Author: Mario Volke, Lothar Braun
"""

import sys
import os
import subprocess
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'vendor'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

import math
import bson
import config
import common
import backend

import operator

from bottle import TEMPLATE_PATH, HTTPError, post, get, run, debug, request, static_file, error, response, redirect
from bottle import jinja2_view as view, jinja2_template as template
from bottle import PasteServer

# set template path
TEMPLATE_PATH.insert(0, os.path.join(os.path.dirname(__file__), "views"))

# get database backend (currently: MongoDB)
db = backend.flowbackend.getBackendObject(config.db_backend, config.db_host, config.db_port, config.db_user, config.db_password, config.db_name)
#dataBackend = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_name)

def extract_mongo_query_params():
	# construct query

	limit = 0
	if "limit" in request.GET:
		try:
			limit = int(request.GET["limit"])
		except ValueError:
			raise HTTPError(output="Param limit has to be an integer.")
		
		if limit < 0:
			limit = 0
			
	fields = None
	if "fields" in request.GET:
		fields = request.GET["fields"].strip()
		fields = map(lambda v: v.strip(), fields.split(","))
		
	sort = None
	if "sort" in request.GET:
		sort = request.GET["sort"].strip()
		sort = map(lambda v: v.strip(), sort.split(","))
		for i in range(0, len(sort)):
			field = sort[i].split(" ")
			order = 1
			if field[-1].lower() == "asc":
				field.pop()
			elif field[-1].lower() == "desc":
				order = -1
				field.pop()
			
			field = " ".join(field)
			sort[i] = (field, order)
			
	count = False
	if "count" in request.GET:
		count = True

	# get query params
	start_bucket = 0
	if "start_bucket" in request.GET:
		try:
			start_bucket = int(request.GET["start_bucket"])
		except ValueError:
			raise HTTPError(output="Param start_bucket has to be an integer.")
		
		if start_bucket < 0:
			start_bucket = 0
	
	end_bucket = sys.maxint
	if "end_bucket" in request.GET:
		try:
			end_bucket = int(request.GET["end_bucket"])
		except ValueError:
			raise HTTPError(output="Param end_bucket has to be an integer.")
		
		if end_bucket < 0:
			end_bucket = 0
	
	# the bucket resolution to query (number of buckets)		
	resolution = 1
	if "resolution" in request.GET:
		try:
			resolution = int(request.GET["resolution"])
		except ValueError:
			raise HTTPError(output="Param resolution has to be an integer.")
		
		if resolution < 1:
			resolution = 1
			
	# or set the bucket size directly
	bucket_size = None
	if "bucket_size" in request.GET:
		try:
			bucket_size = int(request.GET["bucket_size"])
		except ValueError:
			raise HTTPError(output="Param bucket_size has to be an integer.")
			
		if bucket_size not in config.flow_bucket_sizes:
			raise HTTPError(output="This bucket size is not available.")
			
	# biflow aggregation
	# This simply removes the difference between srcIP and dstIP
	# (The smaller ip will always be the srcIP)
	biflow = False
	if "biflow" in request.GET:
		biflow = True


	# protocol filter
	include_protos = []
	if "include_protos" in request.GET:
		include_protos = request.GET["include_protos"].strip()
		include_protos = map(lambda v: common.getValueFromProto(v.strip()), include_protos.split(","))
	exclude_protos = []
	if "exclude_protos" in request.GET:
		exclude_protos = request.GET["exclude_protos"].strip()
		exclude_protos = map(lambda v: common.getValueFromProto(v.strip()), exclude_protos.split(","))

	
	# port filter
	include_ports = []
	if "include_ports" in request.GET:
		include_ports = request.GET["include_ports"].strip()
		try:
			include_ports = map(lambda v: int(v.strip()), include_ports.split(","))
		except ValueError:
			raise HTTPError(output="Ports have to be integers.")
			
	exclude_ports = []
	if "exclude_ports" in request.GET:
		exclude_ports = request.GET["exclude_ports"].strip()
		try:
			exclude_ports = map(lambda v: int(v.strip()), exclude_ports.split(","))
		except ValueError:
			raise HTTPError(output="Ports have to be integers.")
	# ip filter
	include_ips = []
	if "include_ips" in request.GET:
		include_ips = request.GET["include_ips"].strip()
		include_ips = map(lambda v: int(v.strip()), include_ips.split(","))

	exclude_ips = []
	if "exclude_ips" in request.GET:
		exclude_ips = request.GET["exclude_ips"].strip()
		exclude_ips = map(lambda v: int(v.strip()), exclude_ips.split(","))
	
	# get buckets and aggregate
	if bucket_size == None:
		bucket_size = db.getBucketSize(start_bucket, end_bucket, resolution)

	# only stated fields will be available, all others will be aggregated toghether	
	# filter for known aggregation values
	#if fields != None:
	#	fields = [v for v in fields if v in config.flow_aggr_values]
	black_others = False
	if "black_others" in request.GET:
		black_others = True

	aggregate = []
	if "aggregate" in request.GET:
		aggregate = request.GET["aggregate"].strip()
		aggregate = map(lambda v: v.strip(), aggregate.split(","))

	result = {}
	result["fields"] = fields
	print "Fields: " + str(fields)
	result["sort"] = sort
	result["limit"] = limit
	result["count"] = count
	result["start_bucket"] = start_bucket
	result["end_bucket"] = end_bucket
	result["resolution"] = resolution
	result["bucket_size"] = bucket_size
	result["biflow"] = biflow
	result["include_ports"] = include_ports
	result["exclude_ports"] = exclude_ports
	result["include_ips"] = include_ips
	result["exclude_ips"] = exclude_ips
	result["include_protos"] = include_protos
	result["exclude_protos"] = exclude_protos
	result["batch_size"] = 1000
	result["aggregate"] = aggregate
	result["black_others"] = black_others

	return result
			
@get("/")
@get("/dashboard")
@get("/dashboard/:##")
@get("/flow-details")
@get("/flow-details/:##")
@get("/graph")
@get("/graph/:##")
@get("/query-page")
@get("/query-page/:##")
@get("/hierarchical-edge-bundle")
@get("/hierarchical-edge-bundle/:##")
@get("/hive-plot")
@get("/hive-plot/:##")
@get("/ip-documentation")
@get("/ip-documentation/:##")
@view("index")
def index():
    # find js files
    include_js = []
    path = os.path.join(os.path.dirname(__file__), "static", "js", "dev")
    for dirname, dirnames, filenames in os.walk(path):
        dirnames.sort(reverse=True)
        filenames.sort(reverse=True)
        for filename in filenames:
            if not filename.startswith(".") and filename.endswith(".js"):
                include_js.insert(0, dirname[len(os.path.dirname(__file__)):] + "/" + filename)

    # find frontend templates
    frontend_templates = []
    path = os.path.join(os.path.dirname(__file__), "views", "frontend")
    for filename in os.listdir(path):
        if not filename.startswith(".") and filename.endswith(".tpl"):
            frontend_templates.append(os.path.join("frontend", filename))

    return dict(
        include_js = include_js,
        frontend_templates = frontend_templates)

@get("/api/bucket/query")
@get("/api/bucket/query/")
def api_bucket_query():
	query_params = extract_mongo_query_params()
	fields = query_params["fields"]
	bucket_size = query_params["bucket_size"]

	# get proper collection
	collection = None
	if (query_params["fields"] != None and len(query_params["fields"]) > 0)  or len(query_params["include_ports"]) > 0 or len(query_params["exclude_ports"]) > 0 or len(query_params["aggregate"]) > 0:
		collection = db.getCollection(common.DB_FLOW_PREFIX + str(bucket_size))
	else:
		# use preaggregated collection
		collection = db.getCollection(common.DB_FLOW_AGGR_PREFIX + str(bucket_size))

	(buckets, total, min_bucket, max_bucket) = collection.bucket_query(query_params)
	
	return { 
		"bucket_size": query_params["bucket_size"],
		"global_min_bucket": min_bucket,
		"global_max_bucket": max_bucket,
		"results": buckets
	}

@get("/api/dynamic/index/:name")
def api_dynamic_index(name):
	query_params = extract_mongo_query_params()
	(results, total) = db.dynamic_index_query(name, query_params)

	return { "totalCounter" : total, "results": results }

	
@get("/api/index/:name")
@get("/api/index/:name/")
def api_index(name):
	query_params =  extract_mongo_query_params()

	collection = None
	if name == "nodes":
		collection = db.getCollection(common.DB_INDEX_NODES)
	elif name == "ports":
		collection = db.getCollection(common.DB_INDEX_PORTS)
		
	if collection == None:
		raise HTTPError(404, "Index name not known.")

	# static indexes don't konw about start and end times
	# remove them from the query params
	query_params["start_bucket"] = None
	query_params["end_bucket"] = None

	(result, total) = collection.index_query(query_params)

	return { "totalCounter": total, "results": result }

@get("/api/hostinfo/")
def api_hostinfo():
	quer_params =  extract_mongo_query_params()
	data = dataBackend.data_query(common.HOST_INFORMATION_COLLECTION, None)
	return { "results": data }
	
@get("/static/:path#.+#")
def server_static(path):
	return static_file(path, root=os.path.join(os.path.dirname(__file__), "static"))


if __name__ == "__main__":
	#run(server=PasteServer, host=config.host, port=config.port, reloader=config.debug)
	run(host=config.host, port=config.port, reloader=True, debug=config.debug)
	debug(config.debug)
