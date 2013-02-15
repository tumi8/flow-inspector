from  flowbackend import Backend

import sys
import config
import common
import time
import operator


class MongoBackend(Backend):
	def __init__(self, host, port, user, password, databaseName):
		Backend.__init__(self, host, port, user, password, databaseName)
		self.connect()

	def build_spec(self, query_params):
		start_bucket = query_params["start_bucket"]
		end_bucket = query_params["end_bucket"]
		include_ports = query_params["include_ports"]
		exclude_ports = query_params["exclude_ports"]
		include_ips = query_params["include_ips"]
		exclude_ips = query_params["exclude_ips"]

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

		return spec

	def flushCache(self, collectionName = None):
		self.flush_index_cache()

	def connect(self):
		# init pymongo connection
		try:
			import pymongo
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Mongo database: pymongo is not installed!", inst
			sys.exit(1)
		try:
			self.conn = pymongo.Connection(self.host, self.port)
		except pymongo.errors.AutoReconnect, e:
			print >> sys.stderr, "Cannot connect to Mongo Database: ", e
			sys.exit(1)
		self.dst_db = self.conn[self.databaseName]
	

	def getMinBucket(self, bucketSize = None):
		import pymongo
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		coll = self.dst_db[common.DB_FLOW_PREFIX + str(bucketSize)]
		min_bucket = coll.find_one(
			fields={ common.COL_BUCKET: 1, "_id": 0 }, 
			sort=[(common.COL_BUCKET, pymongo.ASCENDING)])
		return min_bucket[common.COL_BUCKET]

	def getMaxBucket(self, bucketSize = None):
		import pymongo
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		coll = self.dst_db[common.DB_FLOW_PREFIX + str(bucketSize)]
		max_bucket = coll.find_one(
			fields={ common.COL_BUCKET: 1, "_id": 0 }, 
			sort=[(common.COL_BUCKET, pymongo.DESCENDING)])
		return max_bucket[common.COL_BUCKET]


	def getBucketSize(self, start_time, end_time, resolution):
		import pymongo
		for i,s in enumerate(config.flow_bucket_sizes):
			if i == len(config.flow_bucket_sizes)-1:
				return s
				
			coll = self.getCollection(common.DB_FLOW_AGGR_PREFIX + str(s))
			min_bucket = coll.find_one(
				{ common.COL_BUCKET: { "$gte": start_time, "$lte": end_time} }, 
				fields={ common.COL_BUCKET: 1, "_id": 0 }, 
				sort=[(common.COL_BUCKET, pymongo.ASCENDING)])
			max_bucket = coll.find_one(
				{ common.COL_BUCKET: { "$gte": start_time, "$lte": end_time} }, 
				fields={ common.COL_BUCKET: 1, "_id": 0 }, 
				sort=[(common.COL_BUCKET, pymongo.DESCENDING)])
				
			if not min_bucket or not max_bucket:
				return s
			
			num_slots = (max_bucket[common.COL_BUCKET]-min_bucket[common.COL_BUCKET]) / s + 1
			if num_slots <= resolution:
				return s

	
	def clearDatabase(self):
		self.conn.drop_database(self.databaseName)


	def createIndex(self, collectionName, fieldName):
		collection = self.dst_db[collectionName]
		collection.create_index(fieldName)

	def update(self, collectionName, statement, document, insertIfNotExists, comes_from_cache = False):
		if collectionName.startswith("index") and not comes_from_cache:
			self.handle_index_update(collectionName, statement, document, insertIfNotExists)
			return 

		collection = self.dst_db[collectionName]
		collection.update(statement, document, insertIfNotExists)

	def bucket_query(self, collectionName,  query_params):
		import pymongo
		collection = self.dst_db[collectionName]
		min_bucket = self.getMinBucket();
		max_bucket = self.getMaxBucket();

		aggregate = query_params["aggregate"]
		if len(aggregate) == 0:
			aggregate.append(common.COL_BUCKET)

		query_params["aggregate"] = aggregate

		(buckets, total) = self.run_query(collection, query_params, originalFlowDb = True, calculateTotals = False)

		return (buckets, None, min_bucket, max_bucket);

	def index_query(self, collectionName, query_params):
		sort = query_params["sort"]
		limit = query_params["limit"]
		fields = query_params["fields"]
		count = query_params["count"]

		spec = self.build_spec(query_params)

		collection = self.dst_db[collectionName]
		# query without the total field	
		full_spec = {}
		full_spec["$and"] = [
				spec, 
				{ common.COL_ID :  { "$ne": "total" } }
			]
	
		cursor = collection.find(full_spec, fields=fields).batch_size(1000)
	
		if sort:
			cursor.sort(sort)
		if limit:
			cursor.limit(limit)
			
		if count:
			result = cursor.count() 
		else:
			result = []
			total = []
			for row in cursor:
				del row["_id"]
				result.append(row)

		# get the total counter
		spec = {common.COL_ID: "total" }
		cursor = collection.find(spec)
		if cursor.count() > 0:
			total = cursor[0]
			del total["_id"]
	
		return (result, total)

	def dynamic_index_query(self, name, query_params):
		import pymongo

		spec = self.build_spec(query_params)
		fields = query_params["fields"]
		sort = query_params["sort"]
		limit = query_params["limit"]
		bucket_size = query_params["bucket_size"]
		aggregate = query_params["aggregate"]
		start_bucket = query_params["start_bucket"]
		end_bucket = query_params["end_bucket"]
		include_ports = query_params["include_ports"]
		exclude_ports = query_params["exclude_ports"]
		include_protos = query_params["include_protos"]
		exclude_protos = query_params["exclude_protos"]
		include_ips = query_params["include_ips"]
		exclude_ips = query_params["exclude_ips"]

		if len(aggregate) > 0:
			# we must calculate all the stuff from our orignial
			# flow db
			collection = self.dst_db[common.DB_FLOW_PREFIX + str(bucket_size)]
			originalFlowDb = True
		else:	
			# we can use the precomuped indexes. *yay*
			if name == "nodes":
				collection = self.dst_db[common.DB_INDEX_NODES + "_" + str(query_params["bucket_size"])]
			elif name == "ports":
				collection = self.dst_db[common.DB_INDEX_PORTS + "_" + str(query_params["bucket_size"])]
			else:
				raise Exception("Unknown dynamic index specified")
			aggregate = [ common.COL_ID ]
			originalFlowDb = False
		query_params["aggregate"] = aggregate

		return self.run_query(collection, query_params, originalFlowDb, calculateTotals = True)

	def run_query(self, collection, query_params, originalFlowDb, calculateTotals):
		import pymongo

		spec = self.build_spec(query_params)
		fields = query_params["fields"]
		sort = query_params["sort"]
		limit = query_params["limit"]
		bucket_size = query_params["bucket_size"]
		aggregate = query_params["aggregate"]
		start_bucket = query_params["start_bucket"]
		end_bucket = query_params["end_bucket"]
		include_ports = query_params["include_ports"]
		exclude_ports = query_params["exclude_ports"]
		include_protos = query_params["include_protos"]
		exclude_protos = query_params["exclude_protos"]
		include_ips = query_params["include_ips"]
		exclude_ips = query_params["exclude_ips"]


		commonFilter = {
			"$match" : {
				"$and": []
			}
		}
		commonFilter["$match"]["$and"].append({"bucket" : { "$gte" : start_bucket, "$lte" : end_bucket }})

		if len(include_protos) > 0:
			commonFilter["$match"]["$and"].append({ common.COL_PROTO: { "$in": include_protos } })

		if len(exclude_protos) > 0:
			commonFilter["$match"]["$and"].append({ common.COL_PROTO: { "$nin": exclude_protos }})

		if len(include_ports) > 0:
			commonFilter["$match"]["$and"].append(
				{ "$or" : [{ common.COL_SRC_PORT: { "$in": include_ports } }, { common.COL_DST_PORT: { "$in": include_ports } }] }
			)

		if len(exclude_ports) > 0:
			commonFilter["$match"]["$and"].append({common.COL_SRC_PORT : { "$nin": exclude_ports }})
			commonFilter["$match"]["$and"].append({common.COL_DST_PORT : { "$nin": exclude_ports }})

		if len(include_ips) > 0:
			commonFilter["$match"]["$and"].append(
				{ "$or" : [{ common.COL_SRC_IP: { "$in": include_ips } }, { common.COL_DST_IP: { "$in": include_ips } }] }
			)

		if len(exclude_ips) > 0:
			commonFilter["$match"]["$and"].append({common.COL_SRC_IP : { "$nin": exclude_ips }})
			commonFilter["$match"]["$and"].append({common.COL_DST_IP : { "$nin": exclude_ips }})

		#mongo aggregation framework pipeline elements
		matchTotalGroup = {
			"$match" : {
				common.COL_ID : "total",
			}
		}

		matchOthers = {
			"$match" : {
				common.COL_ID : { "$ne" : "total" },
			}
		}

		if sort: 
			sort = {
				"$sort" : {
					sort[0][0] : sort[0][1]
				}
			}
		if limit: 
			limit = {
				"$limit" : limit
			}


		aggregateGroup = {
			"$group" : {
				"_id" : {}, 
			}
		}
		doIPAddress = False
		doPort = False
		for field in aggregate:
			if field == common.COL_IPADDRESS:
				doIPAddress = True
			elif field == common.COL_PORT:
				doPort = True
			else: 
				aggregateGroup["$group"]["_id"][field] = "$" +field

		if fields != None:
			for field in fields:
				if field == common.COL_BUCKET: 
					aggregateGroup["$group"][field] = { "$max" : "$" + field }
				else:
					aggregateGroup["$group"][field] = { "$sum" : "$" + field }
		else:
			for s in config.flow_aggr_sums + [ "flows" ]:
				aggregateGroup["$group"][s] = { "$sum" : "$" + s }
				for p in common.AVAILABLE_PROTOS:
					aggregateGroup["$group"][p + "_" + s] = { "$sum" : "$" + p + "." + s }

		if not originalFlowDb:
			# use pre-calculated indexes
			totalPipeline = [ commonFilter,  matchTotalGroup, aggregateGroup ]
			othersPipeline = [ commonFilter, matchOthers, aggregateGroup, sort, limit ]
	
			aggResult =  collection.aggregate(totalPipeline)
			total = aggResult["result"]
			if len(total) > 0:
				if len(total) > 1:
					print "Bug: Got more than one result for total:"
					print total
				total = total[0]
			else:
				print "Did not get total!"
				total =  None
		
			aggResult = collection.aggregate(othersPipeline)
			results = aggResult["result"]
		else:
			# create from flow database
			if doIPAddress:
				src = common.COL_SRC_IP
				dst = common.COL_SRC_IP
				fieldName = common.COL_IPADDRESS
			elif doPort:
				src = common.COL_SRC_PORT
				dst = common.COL_SRC_PORT
				fieldName = common.COL_PORT

			if doIPAddress or doPort:
				projectDual =  {
					"$project" : {
						"_id" : 1,
						"index" : { "$const" :[0,1] }
					}
				}
				for s in config.flow_aggr_values + config.flow_aggr_sums + [ "flows"] + common.AVAILABLE_PROTOS:
					projectDual["$project"][s] = 1
				unwind = {
					"$unwind" : "$index"
				}	
				aggregateGroup["$group"]["_id"][fieldName] = { "$cond" : [ {"$eq":['$index', 0]}, '$' + src, '$' + dst] }
				pipeline = [ commonFilter, projectDual, unwind, aggregateGroup]
				if sort: 
					pipeline.append(sort)
				if limit: 
					pipeline.append(limit)
				aggResult =  collection.aggregate(pipeline)
				results = aggResult["result"]
				if calculateTotals:
					aggregateGroup["$group"]["_id"] = "null"
					pipeline = [ commonFilter, projectDual, unwind, aggregateGroup ]
					aggResult = collection.aggregate(pipeline)
					total = aggResult["result"]
					if len(total) > 0:
						total = total[0]
					else:
						total = None
				else:
					total = None
			else:
				pipeline = [ commonFilter, aggregateGroup ]
				if sort:
					pipeline.append(sort)
				if limit:
					pipeline.append(limit)
				aggResult = collection.aggregate(pipeline)
				results = aggResult["result"]
				if calculateTotals:
					# aggregate all elements from selection
					aggregateGroup["$group"]["_id"] = "null"
					pipeline = [ commonFilter, aggregateGroup ]
					aggResult = collection.aggregate(pipeline)
					total = aggResult["result"]
					if len(total) > 0:
						total = total[0]
					else:
						total = None
				else:
					total = None

		# we need to map the protocol_value fields into protocol : { value : xxx }
		# dictionaries. This must be done for total and results
		for row in results:
			for field in aggregate:
				if  field == common.COL_IPADDRESS or field == common.COL_PORT:
					row["id"] = row["_id"][field]
				else:
					row[field] = row["_id"][field]
			del row["_id"]
			if fields == None: 
				for p in common.AVAILABLE_PROTOS:
					row[p] = {}
					for s in config.flow_aggr_sums + ["flows"]:
						row[p][s] = row[p + "_" + s]
						del row[p + "_" + s]

		if total != None:
			if total["_id"] == "null":
				total["id"] = "total"
			else:
				total["id"] = total["_id"]
				for field in aggregate:
					total[field] = total["_id"][field]
			del total["_id"]
			for p in common.AVAILABLE_PROTOS:
				total[p] = {}
				for s in config.flow_aggr_sums:
					total[p][s] = total[p + "_" + s]
					del total[p + "_" + s]

		return (results, total)
		
	
	def find_one(self, collectionName, spec, fields = None, sort = None):
		collection = self.dst_db[collectionName]
		return collection.find_one(spec, fields=fields, sort=sort)

	def find(self, collectionName,  spec, fields=None, sort=None):
		collection = self.dst_db[collectionName]
		return collection.find(spec, fields=fields, sort=sort)

	def distinct(self, collectionName, field):
		collection = self.dst_db[collectionName]
		return collection.distinct(field)
		
