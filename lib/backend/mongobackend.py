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



	def connect(self):
		# init pymongo connection
		try:
			import pymongo
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Mongo database: pymongo is not installed!"
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

	def update(self, collectionName, statement, document, insertIfNotExists):
		collection = self.dst_db[collectionName]
		collection.update(statement, document, insertIfNotExists)

	def bucket_query(self, collectionName,  query_params):
		import pymongo
		collection = self.dst_db[collectionName]
		spec = self.build_spec(query_params)
		fields = query_params["fields"]
		sort = query_params["sort"]
		include_ips = query_params["include_ips"]
		exclude_ips = query_params["exclude_ips"]
		include_ports = query_params["include_ports"]
		exclude_ports = query_params["exclude_ports"]
		biflow = query_params["biflow"]
		aggregate = query_params["aggregate"]

		min_bucket = self.getMinBucket();
		max_bucket = self.getMaxBucket();

		if (aggregate and len(aggregate) > 0):
			fields = fields + aggregate

		cursor = collection.find(spec, fields=fields).batch_size(1000)
		if sort: 
			cursor.sort(common.COL_BUCKET, sort)
		else:
			cursor.sort(common.COL_BUCKET, pymongo.ASCENDING)

		buckets = []
		if (fields != None and len(fields) > 0) or len(include_ports) > 0 or len(exclude_ports) > 0 or len(include_ips) > 0 or len(exclude_ips) > 0:
			current_bucket = -1
			aggr_buckets = {}
			for doc in cursor:
				if doc[common.COL_BUCKET] > current_bucket:
					for key in aggr_buckets:
						buckets.append(aggr_buckets[key])
					aggr_buckets = {}
					current_bucket = doc[common.COL_BUCKET]
					
				# biflow?
				if biflow and common.COL_SRC_IP in fields and common.COL_DST_IP in fields:
					srcIP = doc.get(common.COL_SRC_IP, None)
					dstIP = doc.get(common.COL_DST_IP, None)
					if srcIP > dstIP:
						doc[common.COL_SRC_IP] = dstIP
						doc[common.COL_DST_IP] = srcIP
				
				# construct aggregation key
				key = str(current_bucket)
				for a in fields:
					key += str(doc.get(a, "x"))
				
				if key not in aggr_buckets:
					bucket = { common.COL_BUCKET: current_bucket }
					for a in fields:
						bucket[a] = doc.get(a, None)
					for s in [common.COL_FLOWS] + config.flow_aggr_sums:
						bucket[s] = 0
					aggr_buckets[key] = bucket
				else:
					bucket = aggr_buckets[key]
				
				for s in [common.COL_FLOWS] + config.flow_aggr_sums:
					bucket[s] += doc.get(s, 0)
				
			for key in aggr_buckets:
				buckets.append(aggr_buckets[key])
		else:
			# cheap operation if nothing has to be aggregated
			for doc in cursor:
				del doc["_id"]
				buckets.append(doc)

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
				{ "_id": { "$ne": { "key":  "total" }} }
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
				row["id"] = row["_id"]["key"]
				del row["_id"]
				result.append(row)

		# get the total counter
		spec = {"_id": {"key": "total" }}
		cursor = collection.find(spec)
		if cursor.count() > 0:
			total = cursor[0]
			total["id"] = total["_id"]["key"]
			del total["_id"]
		print total
	
		return (result, total)

	def dynamic_index_query(self, name, query_params):
		import pymongo

		spec = self.build_spec(query_params)
		fields = query_params["fields"]
		sort = query_params["sort"]
		limit = query_params["limit"]
		bucket_size = query_params["bucket_size"]
		start_bucket = query_params["start_bucket"]
		end_bucket = query_params["end_bucket"]

		if name == "nodes":
			collection = self.dst_db[common.DB_INDEX_NODES + "_" + str(bucket_size)]
		elif name == "ports":
			collection = self.dst_db[common.DB_INDEX_PORTS + "_" + str(bucket_size)]
		else:
			raise HTTPError(output="Unknown dynamic index")

		#mongo aggregation framework pipeline elements
		matchTotalGroup = {
			"$match" : {
				"_id":  { "key": "total" },
			}
		}

		print matchTotalGroup
		pipeline = [ matchTotalGroup ]

		print collection.aggregate(pipeline)

		return (results, total)
		
	
	def find_one(self, collectionName, spec, fields, sort):
		collection = self.dst_db[collectionName]
		return collection.find_one(spec, fields=fields, sort=sort)


