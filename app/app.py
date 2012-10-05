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

import pymongo
import math
import bson
import config
import common
import backend

import operator

from bottle import TEMPLATE_PATH, HTTPError, post, get, run, debug, request, validate, static_file, error, response, redirect
from bottle import jinja2_view as view, jinja2_template as template
from bottle import PasteServer

# set template path
TEMPLATE_PATH.insert(0, os.path.join(os.path.dirname(__file__), "views"))

# get database backend (currently: MongoDB)
db = backend.getBackendObject(config.db_backend, config.db_host, config.db_port, config.db_user, config.db_password, config.db_name)

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
			order = pymongo.ASCENDING
			if field[-1].lower() == "asc":
				field.pop()
			elif field[-1].lower() == "desc":
				order = pymongo.DESCENDING
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
	if fields != None:
		fields = [v for v in fields if v in config.flow_aggr_values]

	spec = {}
	if start_bucket > 0 or end_bucket < sys.maxint:
		spec["bucket"] = {}
		if start_bucket > 0:
			spec["bucket"]["$gte"] = start_bucket
		if end_bucket < sys.maxint:
			spec["bucket"]["$lte"] = end_bucket
	if len(include_ports) > 0:
		spec["$or"] = [
			{ common.COL_SRC_PORT: { "$in": include_ports } },
			{ common.COL_DST_PORT: { "$in": include_ports } }
		]
	if len(exclude_ports) > 0:
		spec[common.COL_SRC_PORT] = { "$nin": exclude_ports }
		spec[common.COL_DST_PORT] = { "$nin": exclude_ports }
	
	if len(include_ips) > 0:
		spec["$or"] = [
			{ common.COL_SRC_IP : { "$in": include_ips } },
			{ common.COL_DST_IP : { "$in": include_ips } }
		]

	if len(exclude_ips) > 0:
		spec[common.COL_SRC_IP] = { "$in": exclude_ips } 
		spec[common.COL_DST_IP] = { "$in": exclude_ips } 

	
	return (spec, fields, sort, limit, count, start_bucket, end_bucket, resolution, bucket_size, biflow, include_ports, exclude_ports, include_ips, exclude_ips)
			
@get("/")
@get("/dashboard")
@get("/dashboard/:##")
@get("/graph")
@get("/graph/:##")
@get("/query-page")
@get("/query-page/:##")
@get("/hierarchical-edge-bundle")
@get("/hierarchical-edge-bundle/:##")
@get("/hive-plot")
@get("/hive-plot/:##")
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
	(spec, fields, sort, limit, count, start_bucket, end_bucket, resolution, bucket_size, biflow, include_ports, exclude_ports, include_ips, exclude_ips)= extract_mongo_query_params()

	# get proper collection
	collection = None
	if (fields != None and len(fields) > 0)  or len(include_ports) > 0 or len(exclude_ports) > 0:
		collection = db.getCollection(common.DB_FLOW_PREFIX + str(bucket_size))
	else:
		# use preaggregated collection
		collection = db.getCollection(common.DB_FLOW_AGGR_PREFIX + str(bucket_size))

	if fields != None:
		query_fields = fields + ["bucket", "flows"] + config.flow_aggr_sums
	else:
		query_fields = ["bucket", "flows"] + config.flow_aggr_sums + common.AVAILABLE_PROTOS 

	buckets = collection.bucket_query(spec, query_fields, sort, limit, count, start_bucket, end_bucket, resolution, bucket_size, biflow, include_ports, exclude_ports, include_ips, exclude_ips, 1000)
	
	return { 
		"bucket_size": bucket_size,
		"results": buckets
	}

@get("/api/dynamic/index/:name")
def api_dynamic_index(name):
	def createNewIndexEntry(row):
		# top level
		r = { "id": row[key[0]], "flows": 0 }
		for s in config.flow_aggr_sums:
			r[s] = row[s]

		# protocol specific
		for p in common.AVAILABLE_PROTOS:
			r[p] = { "flows": 0 }
			for s in config.flow_aggr_sums:	
				r[p][s] = 0
		# src and dst specific		
		for dest in ["src", "dst"]:
			r[dest] = {}
			r[dest]["flows" ] = 0
			for s in config.flow_aggr_sums:
				r[dest][s] = 0
		return r

	(spec, fields, sort, limit, count, start_bucket, end_bucket, resolution, bucket_size, biflow, include_ports, exclude_ports, include_ips, exclude_ips)= extract_mongo_query_params()

	collection = db.getCollection(common.DB_FLOW_PREFIX + str(bucket_size))

	cursor = collection.find(spec, fields=fields).batch_size(1000)


	result = {}

	# total counter that contains information about all flows in 
	# the REQUESTED buckets (not over the complete dataset)
	# this is important because the limit parameter might remove
	# some information
	total = {}
	total["flows"] = 0
	for s in config.flow_aggr_sums:
		total[s] = 0
	for proto in common.AVAILABLE_PROTOS:
		total[proto] = {}
		total[proto]["flows"] = 0
		for s in config.flow_aggr_sums:
			total[proto][s] = 0

	for row in cursor:
		if name == "nodes":
			keylist = [ (common.COL_SRC_IP, "src"), (common.COL_DST_IP, "dst") ]
		elif name == "ports":
			keylist = [ (common.COL_SRC_PORT, "src"), (common.COL_DST_PORT, "dst") ]
		else:
			raise HTTPError(output = "Unknown dynamic index")

		# update total counters
		total["flows"] += row["flows"]
		if common.COL_PROTO in row:
			total[common.getProtoFromValue(row[common.COL_PROTO])]["flows"] += row["flows"]
		for s in config.flow_aggr_sums:
			total[s] += row[s]
			if common.COL_PROTO in row:
				total[common.getProtoFromValue(row[common.COL_PROTO])][s] += row[s]


		# update individual counters
		for key in keylist:
			if row[key[0]] in result:
				r = result[row[key[0]]]
			else:
				r = createNewIndexEntry(row)

			r["flows"] += row["flows"]
			r[key[1]]["flows"] += row["flows"]
			for s in config.flow_aggr_sums:
				r[s] += row[s]
				r[key[1]][s] += row[s]

			if common.COL_PROTO in row:
				r[common.getProtoFromValue(row[common.COL_PROTO])]["flows"] += row["flows"]
				for s in config.flow_aggr_sums:
					r[common.getProtoFromValue(row[common.COL_PROTO])][s] += row[s]

			result[row[key[0]]] = r

	# no that we have calculated the indexes, take the values and postprocess them
	results = result.values()
	if sort:
		# TODO: implement sort function that allows for sorting with two keys
		if len(sort) != 1:
			raise HTTPError(output = "Cannot sort by multiple fields. This must yet be implemented.")
		if sort[0][1] == pymongo.ASCENDING:
			results.sort(key=operator.itemgetter(sort[0][0]))
		else:
			results.sort(key=operator.itemgetter(sort[0][0]), reverse=True)
	
	if limit:
		results = results[0:limit]

	return { "totalCounter" : total, "results": results }

	
@get("/api/index/:name")
@get("/api/index/:name/")
def api_index(name):
	(spec, fields, sort, limit, count, start_bucket, end_bucket, resolution, bucket_size, biflow, include_ports, exclude_ports, include_ips, exclude_ips)= extract_mongo_query_params()

	collection = None
	if name == "nodes":
		collection = db.getCollection(common.DB_INDEX_NODES)
	elif name == "ports":
		collection = db.getCollection(common.DB_INDEX_PORTS)
		
	if collection == None:
		raise HTTPError(404, "Index name not known.")


	(result, total) = collection.index_query(spec, fields, sort, limit, count, start_bucket, end_bucket, resolution, bucket_size, biflow, include_ports, exclude_ports, include_ips, exclude_ips, 1000)

	return { "totalCounter": total, "results": result }

@get("/static/:path#.+#")
def server_static(path):
	return static_file(path, root=os.path.join(os.path.dirname(__file__), "static"))


if __name__ == "__main__":
	#run(server=PasteServer, host=config.host, port=config.port, reloader=config.debug)
	run(host=config.host, port=config.port, reloader=config.debug)
	debug(config.debug)
