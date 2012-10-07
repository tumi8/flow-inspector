"""
Backend wrappers. Select and create the appropriate objects for 
backend communication.

Currently supported backends:
	- mongodb
	- mysql

Author: Lothar Braun 
"""

import sys
import config
import common

class Collection:
	"""
		This class is a wrapper for the mongo db collection classes
		it takes requests for storing and querying data from the
		application and passes it to the backend classes, which are
		then responsible for storing and retriving the data in/from 
		the actual database.
		For backends other than mongo, a collection represents a 
		table (SQL) or another appropriate collection for flows that
		belong to the same cagegory
	"""
	def __init__(self, backendObject, collectionName):
		self.backendObject = backendObject
		self.collectionName = collectionName
		self.name = collectionName

	def createIndex(self, fieldName):
		"""
		Creates an index on the given field if the database 
		supports indexes
		"""
		self.backendObject.createIndex(self.collectionName, fieldName)

	def update(self, statement, document, insertIfNotExist):
		"""
		Updates or creates a flow in the database. 
		- statement - contains the id of the flow in the database (_id for mongo, primary key for sql)
		- document - contains the properties of the flow
		- insertIfNotExist: true: insert flow into db if it doesn't exist; false: only update entries of existing flows, do not create new flow
		"""
		self.backendObject.update(self.collectionName, statement, document, insertIfNotExist)

	def bucket_query(self, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		"""
		Queries the database for a set of flows that match the given properties
		- spec - mongo specific spec for the query. already encodes the arguments (applies only to mongo)
		- fields - database fields that should be queried and returned by the database
		- sort - list of fields that should be sorted, including the sort direction
		- count - maximum number of flows that should be returned
		- startBucket: start interval for flows
		- endBucket: end interval for flow queries
		- resolution: desired time resolution
		- bucketSize: size of buckets to be queried
		- biflow: whether we should have biflow aggregation or not
		- includePorts: only select flows that involve the ports in this list
		- excludePorts: only select flows that do not involve the ports in this list
		- inludeIPs: only select flows that involve the ips in this list
		- excludeIPs: only select flows that do not involve the ips in this list
		- batchSize: database should select the flows in batch sizes of batchSize (ignored for most backends)
		"""
		return self.backendObject.bucket_query(self.collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize)

	def index_query(self, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		"""
		Queries the database for static indexes on all flows in the database
		- spec - mongo specific spec for the query. already encodes the arguments (applies only to mongo)
		- fields - database fields that should be queried and returned by the database
		- sort - list of fields that should be sorted, including the sort direction
		- count - maximum number of flows that should be returned
		- startBucket: start interval for flows
		- endBucket: end interval for flow queries
		- resolution: desired time resolution
		- bucketSize: size of buckets to be queried
		- biflow: whether we should have biflow aggregation or not
		- includePorts: only select flows that involve the ports in this list
		- excludePorts: only select flows that do not involve the ports in this list
		- inludeIPs: only select flows that involve the ips in this list
		- excludeIPs: only select flows that do not involve the ips in this list
		- batchSize: database should select the flows in batch sizes of batchSize (ignored for most backends)
		"""
		return self.backendObject.index_query(self.collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize)
 

	def find_one(self, spec, fields=None, sort=None):
		return self.backendObject.find_one(self.collectionName, spec, fields, sort)

class Backend:
	def __init__(self, conn, backendType, databaseName):
		self.conn = conn
		self.backendType = backendType
		self.databaseName = databaseName


	def getBucketSize(self, startTime, endTime, resolution):
		"""
		Calculates the bucket size that is closest to the requested resolution.
		The resolution is specified by the javascript client application and
		can be picked to be an arbitrary value.
		The bucket sizes are defined in config.py, and must not necessarily
		match to the requested resolution.
		"""
		pass

	def clearDatabase(self):
		"""
		Removes all data from the backend
		"""
		pass

	def getCollection(self, name):
		"""
		Creates and returns a wrapper "Collection" object which represents
		a collection/table in the database. 
		"""
		return Collection(self, name)
	
	def prepareCollections(self):
		"""
		This method is responsible for initializing the database on application 
		start. Some databases need to create tables when the database is used
		for the first time. Others require maintainance or sanity checking on 
		before the database can be used (e.g. writes). 
		All such initialization is done in this method. The method is only 
		called for writing processes (e.g. the preprocessor)
		"""
		pass

	def createIndex(self, collectionName, fieldName):
		"""
		Creates an index on the collection/table "collectionName" on the
		field "fieldName", if the database supports creating indexes.
		"""
		pass

	def update(self, collectionName, statement, document, insertIfNotExists):
		"""
		Adds or updates a flow in the collection "collectionName". See class
		Collection for full documentation. 
		"""
		pass

	def bucket_query(self, collectionName,  spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		"""
		Queries flows from the database and collection "colletionName". See
		class Collection for full documentation of the parameters.
		"""
		pass

	def index_query(self, collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		"""
		Queries static indexes on the full database. See class Collection for 
		full documentation.
		"""
		pass

	def find_one(self, collectionName, spec, fields, sort):
		pass


class MongoBackend(Backend):
	def __init__(self, conn, backendType, databaseName):
		Backend.__init__(self, conn, backendType, databaseName)
		self.dst_db = self.conn[databaseName]

	def getBucketSize(self, start_time, end_time, resolution):
		import pymongo
		for i,s in enumerate(config.flow_bucket_sizes):
			if i == len(config.flow_bucket_sizes)-1:
				return s
				
			coll = self.getCollection(common.DB_FLOW_AGGR_PREFIX + str(s))
			min_bucket = coll.find_one(
				{ "bucket": { "$gte": start_time, "$lte": end_time} }, 
				fields={ "bucket": 1, "_id": 0 }, 
				sort=[("bucket", pymongo.ASCENDING)])
			max_bucket = coll.find_one(
				{ "bucket": { "$gte": start_time, "$lte": end_time} }, 
				fields={ "bucket": 1, "_id": 0 }, 
				sort=[("bucket", pymongo.DESCENDING)])
				
			if not min_bucket or not max_bucket:
				return s
			
			num_slots = (max_bucket["bucket"]-min_bucket["bucket"]) / s + 1
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

	def bucket_query(self, collectionName,  spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		import pymongo
		collection = self.dst_db[collectionName]
		cursor = collection.find(spec, fields=fields).batch_size(1000)
		if sort: 
			cursor.sort("bucket", sort)
		else:
			cursor.sort("bucket", pymongo.ASCENDING)

		buckets = []
		if (fields != None and len(fields) > 0) or len(includePorts) > 0 or len(excludePorts) > 0 or len(includeIps) > 0 or len(excludeIps) > 0:
			current_bucket = -1
			aggr_buckets = {}
			for doc in cursor:
				if doc["bucket"] > current_bucket:
					for key in aggr_buckets:
						buckets.append(aggr_buckets[key])
					aggr_buckets = {}
					current_bucket = doc["bucket"]
					
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
					bucket = { "bucket": current_bucket }
					for a in fields:
						bucket[a] = doc.get(a, None)
					for s in ["flows"] + config.flow_aggr_sums:
						bucket[s] = 0
					aggr_buckets[key] = bucket
				else:
					bucket = aggr_buckets[key]
				
				for s in ["flows"] + config.flow_aggr_sums:
					bucket[s] += doc.get(s, 0)
				
			for key in aggr_buckets:
				buckets.append(aggr_buckets[key])
		else:
			# cheap operation if nothing has to be aggregated
			for doc in cursor:
				del doc["_id"]
				buckets.append(doc)
		return buckets

	def index_query(self, collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		collection = self.dst_db[collectionName]
		# query without the total field	
		full_spec = {}
		full_spec["$and"] = [
				spec, 
				{ "_id": { "$ne": "total" }}
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
				row["id"] = row["_id"]
				del row["_id"]
				result.append(row)
	
		# get the total counter
		spec = {"_id": "total"}
		cursor = collection.find(spec)
		if cursor.count() > 0:
			total = cursor[0]
			total["id"] = total["_id"]
			del total["_id"]
	
		return (result, total)

	def find_one(self, collectionName, spec, fields, sort):
		collection = self.dst_db[collectionName]
		return collection.find_one(spec, fields=fields, sort=sort)



class MysqlBackend(Backend):
	def __init__(self, conn, backendType, databaseName):
		import MySQLdb
		Backend.__init__(self, conn, backendType, databaseName)
		self.cursor = conn.cursor(MySQLdb.cursors.DictCursor)

	
	def clearDatabase(self):
		self.cursor.execute("SHOW TABLES")	
		tables = self.cursor.fetchall()
		for table in tables:
			self.cursor.execute("DROP TABLE " + table[0])

	def prepareCollections(self):
		# we need to create several tables that contain flows and
		# pre aggregated flows, as well as tables for indexes that 
		# are precalculated on the complete data set. 

		# tables for storing bucketized flow data
		for s in config.flow_bucket_sizes:
			createString = "CREATE TABLE IF NOT EXISTS "
			createString += common.DB_FLOW_PREFIX + str(s) + " (_id VARBINARY(120) NOT NULL, bucket INTEGER(10) UNSIGNED NOT NULL"
			for v in config.flow_aggr_values:
				createString += ", %s %s NOT NULL" % (v, common.MYSQL_TYPE_MAPPER[v])
			for s in config.flow_aggr_sums + [ "flows" ]:
				createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ "flows" ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
			createString += ", PRIMARY KEY(_id))"
			self.cursor.execute(createString)

		# tables for storing aggregated timebased data
		for s in config.flow_bucket_sizes:
			createString = "CREATE TABLE IF NOT EXISTS "
			createString += common.DB_FLOW_AGGR_PREFIX + str(s) + " (_id VARBINARY(120) NOT NULL, bucket INTEGER(10) UNSIGNED NOT NULL"
			for s in config.flow_aggr_sums + [ "flows" ]:
				createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ "flows" ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
			createString += ", PRIMARY KEY(_id))"
			self.cursor.execute(createString)
		
		# create precomputed index that describe the whole data set
		for table in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
			createString = "CREATE TABLE IF NOT EXISTS %s (_id INTEGER(10) UNSIGNED NOT NULL" % table
			for s in config.flow_aggr_sums + [ "flows" ]:
				createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ "flows" ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
			for direction in [ "src", "dst" ]:
				for s in config.flow_aggr_sums + [ "flows" ]:
					createString += ", %s %s DEFAULT 0" % (direction + "_" + s, common.MYSQL_TYPE_MAPPER[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ "flows" ]: 
						createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])

			createString += ", PRIMARY KEY(_id))"
			self.cursor.execute(createString)

	def createIndex(self, tableName, fieldName):
		self.cursor.execute("CREATE INDEX %s on %s (%s)" % (fieldName, tableName, fieldName))
		
	def update(self, collectionName, statement, document, insertIfNotExists):
		typeString = "_id"
		valueString = str(statement["_id"])
		if valueString == "total":
			valueString = "0"
		updateString = ""
		for part in [ "$set", "$inc" ]:
			if not part in document:
				continue
			for v in document[part]:
				valueString += "," + str(document[part][v])
				v = v.replace('.', '_')
				typeString += "," + v
				if part == "$inc":
					if updateString != "":
						updateString += ","
					updateString +=  v + "=" + v + "+VALUES(" + v + ")"
		queryString = "INSERT INTO " + collectionName + "(" + typeString + ") VALUES (" + valueString + ") ON DUPLICATE KEY UPDATE " + updateString;
		self.cursor.execute(queryString)

	def getBucketSize(self, startTime, endTime, resolution):
		for i,s in enumerate(config.flow_bucket_sizes):
			if i == len(config.flow_bucket_sizes)-1:
				return s
				
			tableName = common.DB_FLOW_AGGR_PREFIX + str(s)
			queryString = "SELECT bucket FROM %s WHERE bucket >= %d AND bucket <= %d ORDER BY bucket ASC LIMIT 1" % (tableName, startTime, endTime)
			self.cursor.execute(queryString);
			tmp = self.cursor.fetchall()
			minBucket = None
			if len(tmp) > 0:
				minBucket = tmp[0]["bucket"]

			queryString = "SELECT bucket FROM %s WHERE bucket >= %d AND bucket <= %d ORDER BY bucket DESC LIMIT 1" % (tableName, startTime, endTime)
			self.cursor.execute(queryString);
			tmp = self.cursor.fetchall()
			maxBucket = None
			if len(tmp) > 0:
				maxBucket = tmp[0]["bucket"]
			
			if not minBucket or not maxBucket:
				return s
			
			numSlots = (maxBucket-minBucket) / s + 1
			if numSlots <= resolution:
				return s

	def bucket_query(self, collectionName,  spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		print "MySQL: Constructing query ... "
		(result, total) = self.sql_query(collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize)
		#print result
		print "MySQL: Returning data to app ..."
		return result

	def index_query(self, collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		return self.sql_query(collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize)


	def sql_query(self, collectionName, spec, fields, sort, limit, count, startBucket, endBucket, resolution, bucketSize, biflow, includePorts, excludePorts, includeIPs, excludeIPs, batchSize):
		fieldList = ""
		if fields != None: 
			for field in fields:
				if field in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ "flows" ]:
						if fieldList != "":
							fieldList += ","
						fieldList += field + "_" + s
				else:
					if fieldList != "":
						fieldList += ","
					fieldList += field
			if not "bucket" in fields:
				fieldList += ",bucket"
		else:
			fieldList = "*"

		isWhere = False
		queryString = "SELECT %s FROM %s " % (fieldList, collectionName)
		if startBucket > 0:
			isWhere = True
			queryString += "WHERE bucket >= %d " % (startBucket)
		if endBucket < sys.maxint:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else: 
				queryString += "AND "
			queryString += "bucket <= %d " % (endBucket)

		firstIncludePort = True
		for port in includePorts:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstIncludePort: 
					queryString += "AND ("
					firstIncludePort = False
				else:
					queryString += "OR "
			queryString += "%s = %d OR %s = %d " % (common.COL_SRC_PORT, port, common.COL_DST_PORT, port)
		if not firstIncludePort:
			queryString += ") "

		firstExcludePort = True
		for port in excludePorts:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstExcludePort: 
					queryString += "AND ("
					firstExcludePort = False
				else:
					queryString += "OR "
			queryString += "%s != %d OR %s != %d " % (common.COL_SRC_PORT, port, common.COL_DST_PORT, port)
		if not firstExcludePort:
			queryString += ") "

		firstIncludeIP = True
		for ip in includeIPs:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstIncludeIP: 
					queryString += "AND ("
					firstIncludeIP = False
				else:
					queryString += "OR "
			queryString += "%s = %d OR %s = %d " % (common.COL_SRC_IP, ip, common.COL_DST_IP, ip)
		if not firstIncludeIP:
			queryString += ") "


		firstExcludeIP = True
		for ip in excludeIPs:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstExcludeIP: 
					queryString += "AND ("
					firstExcludeIP = False
				else:
					queryString += "OR "
			queryString += "%s != %d OR %s != %d " % (common.COL_SRC_IP, ip, common.COL_DST_IP, ip)
		if not firstExcludeIP:
			queryString += ") "

		if sort:
			queryString += "ORDER BY " + sort[0][0] + " "
			if sort[0][1] == 1:
				queryString += "ASC "
			else: 
				queryString += "DESC "

		if limit:
			queryString += "LIMIT %d" % (limit)

		print "MySQL: Running Query ..."
		#print queryString
		self.cursor.execute(queryString)
		queryResult =  self.cursor.fetchall()
		print "MySQL: Encoding Query ..."
		result = []
		total = dict()
		for row in queryResult:
			resultDoc = dict()
			isTotal = False
			for field in row:
				if field == "_id":
					if row[field] == 0:
						isTotal = True
					fieldParts = []
				else: 
					fieldParts = field.split('_')
				if len(fieldParts) > 1:
					# maximum depth is 3 
					# TODO: hacky ... 
					if len(fieldParts) == 2:
						if not fieldParts[0] in resultDoc:
							resultDoc[fieldParts[0]] = dict()
						resultDoc[fieldParts[0]][fieldParts[1]] = row[field]
					elif len(fieldParts) == 3:
						# TODO: not implemented ... 
						pass
					else:
						raise Exception("Internal Programming Error!")
				else:
					if field == "_id":
						resultDoc["id"] = row[field]
					else: 
						resultDoc[field] = row[field]
			if isTotal:
				total = resultDoc
			else:
				result.append(resultDoc)

		print "Got Results: ", len(result)
		print "Total: ", total
		#print "Result: ", result
		#for r in result: 
		#	print r
		return (result, total)


def getBackendObject(backend, host, port, user, password, databaseName):
	if backend == "mongo":
		# init pymongo connection
		try:
			import pymongo
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Mongo database: pymongo is not installed!"
			sys.exit(1)
		try:
			dst_conn = pymongo.Connection(host, port)
		except pymongo.errors.AutoReconnect, e:
			print >> sys.stderr, "Cannot connect to Mongo Database: ", e
			sys.exit(1)
		return MongoBackend(dst_conn, backend, databaseName)
	elif backend == "mysql":
		try:
			import MySQLdb
			import _mysql_exceptions

			dns = dict(
				db = databaseName,
				host = host,
				port = port,
				user = user,
				passwd = password
                        )
                        conn = MySQLdb.connect(**dns)
			return MysqlBackend(conn, backend, databaseName)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to MySQL database: ", inst 
			sys.exit(1)
	else:
		raise Exception("Backend " + backend + " is not a supported backend")
