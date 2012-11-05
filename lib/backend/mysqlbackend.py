from flowbackend import Backend

import sys
import config
import common
import time
import operator

class MysqlBackend(Backend):
	def __init__(self, host, port, user, password, databaseName):
		import MySQLdb
		Backend.__init__(self, host, port, user, password, databaseName)
		self.tableInsertCache = dict()
		self.cachingThreshold = 10000
		self.counter = 0

		self.connect()

		self.doCache = True

	def connect(self):
		import MySQLdb
		import _mysql_exceptions
		print "Connecting ..."
		try:
			dns = dict(
				db = self.databaseName,
				host = self.host,
				port = self.port,
				user = self.user,
				passwd = self.password
			)         
			self.conn = MySQLdb.connect(**dns)
			self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to MySQL database: ", inst 
			sys.exit(1)


	def execute(self, string):
		import MySQLdb
		import _mysql_exceptions
		try: 
			#print string
			self.cursor.execute(string)
		except MySQLdb.OperationalError as e:
			print "Tried statement: ", string
			print "Got error: ", e
			error, = e.args
			if error.code == 1061:
				# index does already exist.
				return
			print "Trying again ..."
			self.connect()
			self.execute(string)
		except MySQLdb.ProgrammingError as e:
			print "Programming Error: ", e
			return {}

	def executemany(self, string, objects):
		import MySQLdb
		import _mysql_exceptions
		#print string
		try:
			self.cursor.executemany(string, objects)
		except (AttributeError, MySQLdb.OperationalError) as e:
			print string
			print e
			self.connect()
			self.executemany(string, objects)

	
	def clearDatabase(self):
		self.execute("SHOW TABLES")	
		tables = self.cursor.fetchall()
		for table in tables:
			self.execute("DROP TABLE " + table.values()[0])

	def prepareCollections(self):
		# we need to create several tables that contain flows and
		# pre aggregated flows, as well as tables for indexes that 
		# are precalculated on the complete data set. 

		# tables for storing bucketized flow data
		for s in config.flow_bucket_sizes:
			primary = common.COL_BUCKET
			createString = "CREATE TABLE IF NOT EXISTS "
			createString += common.DB_FLOW_PREFIX + str(s) + " (" + common.COL_BUCKET + " INTEGER(10) UNSIGNED NOT NULL"
			for v in config.flow_aggr_values:
				primary += "," + v
				createString += ", %s %s NOT NULL" % (v, common.MYSQL_TYPE_MAPPER[v])
			for s in config.flow_aggr_sums + [ "flows" ]:
				createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
			createString += ", PRIMARY KEY(" + primary + "))"
			self.execute(createString)

		# tables for storing aggregated timebased data
		for s in config.flow_bucket_sizes:
			createString = "CREATE TABLE IF NOT EXISTS "
			createString += common.DB_FLOW_AGGR_PREFIX + str(s) + " (" + common.COL_BUCKET + " INTEGER(10) UNSIGNED NOT NULL"
			for s in config.flow_aggr_sums + [common.COL_FLOWS]:
				createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
			createString += ", PRIMARY KEY(" + common.COL_BUCKET + "))"
			self.execute(createString)
		
		# create precomputed index that describe the whole data set
		for table in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
			createString = "CREATE TABLE IF NOT EXISTS %s (%s INTEGER(10) UNSIGNED NOT NULL" % (table, common.COL_ID)
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
			for direction in [ "src", "dst" ]:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
					createString += ", %s %s DEFAULT 0" % (direction + "_" + s, common.MYSQL_TYPE_MAPPER[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
						createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])

			createString += ", PRIMARY KEY(%s))" % common.COL_ID
			self.execute(createString)

		# create tables for storing incremental indexes
		for bucket_size in config.flow_bucket_sizes:
			for index in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
				table = index + "_" + str(bucket_size)
				createString = "CREATE TABLE IF NOT EXISTS " + table + " (" + common.COL_ID + " INTEGER(10) UNSIGNED NOT NULL," + common.COL_BUCKET + " INTEGER(10) UNSIGNED NOT NULL"
				for s in config.flow_aggr_sums + [common.COL_FLOWS]:
					createString += ", %s %s DEFAULT 0" % (s, common.MYSQL_TYPE_MAPPER[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [common.COL_FLOWS ]: 
						createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
				for direction in [ "src", "dst" ]:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
						createString += ", %s %s DEFAULT 0" % (direction + "_" + s, common.MYSQL_TYPE_MAPPER[s])
					for proto in common.AVAILABLE_PROTOS:
						for s in config.flow_aggr_sums + [common.COL_FLOWS ]: 
							createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, common.MYSQL_TYPE_MAPPER[s])
	
				createString += ", PRIMARY KEY(" + common.COL_ID + "," + common.COL_BUCKET + "))"
				self.execute(createString)


	def createIndex(self, tableName, fieldName):
		try:
			self.execute("CREATE INDEX %s on %s (%s)" % (fieldName + "_index", tableName, fieldName))
		except:
			# most common cause: index does already exists
			# TODO check for errors
			pass

		
	def update(self, collectionName, statement, document, insertIfNotExists):
		# special handling for the _id field
		for s in statement:
			if collectionName.startswith('index'):
				if s == common.COL_ID:
					typeString = s
					value = str(statement[s])
					if self.doCache: 
						if value == "total":
							# special value 0 encodes the total field
							cacheLine = (0,)
						else:
							cacheLine = (value,)
						valueString = "%s"
					else:
						if value == "total":
							valueString = "0"
						else:
							valueString = str(value)
				if s == common.COL_BUCKET:
					bucketValue = statement[s]
					if not "$set" in document:
						document["$set"] = {}
					document["$set"][common.COL_BUCKET] = bucketValue
			else:
				typeString = ""
				valueString = ""
				cacheLine = ()
		

		#if not collectionName ==common.DB_INDEX_NODES and not collectionName == common.DB_INDEX_PORTS:
		#	print collectionName,  statement, document
		updateString = ""
		for part in [ "$set", "$inc" ]:
			if not part in document:
				continue
			for v in document[part]:
				if self.doCache:
					cacheLine = cacheLine + (document[part][v],)
					if valueString != "":
						valueString += ","
					valueString += "%s"
				else:
					if valueString != "":
						valueString += ","
					valueString += str(document[part][v])
				v = v.replace('.', '_')
				if typeString != "":
					typeString += "," 
				typeString += v
				if part == "$inc":
					if updateString != "":
						updateString += ","
					updateString +=  v + "=" + v + "+VALUES(" + v + ")"

		queryString = "INSERT INTO " + collectionName + "(" + typeString + ") VALUES (" + valueString + ") ON DUPLICATE KEY UPDATE " + updateString

		if self.doCache:
			numElem = 1
			if collectionName in self.tableInsertCache:
				cache = self.tableInsertCache[collectionName][0]
				numElem = self.tableInsertCache[collectionName][1] + 1
				if queryString in cache:
					cache[queryString].append(cacheLine)
				else:
					cache[queryString] = [ cacheLine ]
			else:
				cache = dict()
				cache[queryString] = [ cacheLine ]
		
			self.tableInsertCache[collectionName] = (cache, numElem)

			self.counter += 1
			#if self.counter % 100000 == 0:
				#print "Total len:",  len(self.tableInsertCache)
				#for c in self.tableInsertCache:
					#print c, len(self.tableInsertCache[c][0]), self.tableInsertCache[c][1]
			
			if numElem > self.cachingThreshold:
				self.flushCache(collectionName)
		else:
			self.execute(queryString)

	def flushCache(self, collectionName=None):
		if collectionName:
			cache = self.tableInsertCache[collectionName][0]
			for queryString in cache:
				cacheLines = cache[queryString]
				self.executemany(queryString, cacheLines)
			del self.tableInsertCache[collectionName]
		else:
			# flush all collections
			while len( self.tableInsertCache) > 0:
				collection = self.tableInsertCache.keys()[0]
				self.flushCache(collection)

	def getMinBucket(self, bucketSize = None):
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		tableName = common.DB_FLOW_PREFIX + str(bucketSize)
		self.execute("SELECT MIN(" + common.COL_BUCKET +") as " + common.COL_BUCKET + " FROM %s" % (tableName))
		return self.cursor.fetchall()[0][common.COL_BUCKET]
		
	def getMaxBucket(self, bucketSize = None):
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		tableName = common.DB_FLOW_PREFIX + str(bucketSize)
		self.execute("SELECT MAX(" + common.COL_BUCKET + ") as " +common.COL_BUCKET + " FROM %s" % (tableName))
		return self.cursor.fetchall()[0][common.COL_BUCKET]

	def getBucketSize(self, startTime, endTime, resolution):
		for i,s in enumerate(config.flow_bucket_sizes):
			if i == len(config.flow_bucket_sizes)-1:
				return s
				
			tableName = common.DB_FLOW_AGGR_PREFIX + str(s)
			queryString = "SELECT " + common.COL_BUCKET + " FROM %s WHERE " + common.COL_BUCKET + " >= %d AND " + common.COL_BUCKEt + " <= %d ORDER BY " + common.COL_BUCKET + " ASC LIMIT 1" % (tableName, startTime, endTime)
			self.execute(queryString);
			tmp = self.cursor.fetchall()
			minBucket = None
			if len(tmp) > 0:
				minBucket = tmp[0][common.COL_BUCKET]

			queryString = "SELECT " + common.COL_BUCKET + " FROM %s WHERE " + common.COL_BUCKET + " >= %d AND " + common.COL_BUCKET + " <= %d ORDER BY " + common.COL_BUCKET + " DESC LIMIT 1" % (tableName, startTime, endTime)
			self.execute(queryString);
			tmp = self.cursor.fetchall()
			maxBucket = None
			if len(tmp) > 0:
				maxBucket = tmp[0][common.COL_BUCKET]
			
			if not minBucket or not maxBucket:
				return s
			
			numSlots = (maxBucket-minBucket) / s + 1
			if numSlots <= resolution:
				return s

	def bucket_query(self, collectionName, query_params):
		print "MySQL: Constructing query ... "
		min_bucket = self.getMinBucket()
		max_bucket = self.getMaxBucket()
		(result, total) = self.sql_query(collectionName, query_params)
		print "MySQL: Returning data to app ..."
		return (result, total, min_bucket, max_bucket);

	def index_query(self, collectionName, query_params):
		return self.sql_query(collectionName, query_params)

	def dynamic_index_query(self, name, query_params):
		aggregate = query_params["aggregate"]
		bucket_size = query_params["bucket_size"]

		if len(aggregate) > 0:
			# we must calculate all the stuff from our orignial
			# flow db
			tableName = common.DB_FLOW_PREFIX + str(bucket_size)
		else:	
			# we can use the precomuped indexes. *yay*
			if name == "nodes":
				tableName = common.DB_INDEX_NODES + "_" + str(query_params["bucket_size"])
			elif name == "ports":
				tableName = common.DB_INDEX_PORTS + "_" + str(query_params["bucket_size"])
			else:
				raise Exception("Unknown index specified")
			query_params["aggregate"] = [ common.COL_ID ]

		(results, total) =  self.sql_query(tableName, query_params)
		return (results, total)

	def sql_query(self, collectionName, query_params):
		fields = query_params["fields"]
		sort  = query_params["sort"]
		limit = query_params["limit"] 
		count = query_params["count"]
		start_bucket = query_params["start_bucket"]
		end_bucket = query_params["end_bucket"]
		resolution = query_params["resolution"]
		bucketSize = query_params["bucket_size"]
		biflow = query_params["biflow"]
		includePorts = query_params["include_ports"]
		excludePorts = query_params["exclude_ports"]
		include_ips = query_params["include_ips"]
		exclude_ips = query_params["exclude_ips"]
		include_protos = query_params["include_protos"]
		exclude_protos = query_params["exclude_protos"]
		batchSize = query_params["batch_size"]  
		aggregate = query_params["aggregate"]
		black_others = query_params["black_others"]


		# create filter strings

		isWhere = False
		queryString = ""
		if start_bucket != None and start_bucket > 0:
			isWhere = True
			queryString += "WHERE " + common.COL_BUCKET + " >= %d " % (start_bucket)
		if end_bucket != None and end_bucket < sys.maxint:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else: 
				queryString += "AND "
			queryString += common.COL_BUCKET + " <= %d " % (end_bucket)

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
					queryString += "AND "
			queryString += "%s != %d AND %s != %d " % (common.COL_SRC_PORT, port, common.COL_DST_PORT, port)
		if not firstExcludePort:
			queryString += ") "

		firstIncludeIP = True
		for ip in include_ips:
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
		for ip in exclude_ips:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstExcludeIP: 
					queryString += "AND ("
					firstExcludeIP = False
				else:
					queryString += "AND "
			queryString += "%s != %d AND %s != %d " % (common.COL_SRC_IP, ip, common.COL_DST_IP, ip)
		if not firstExcludeIP:
			queryString += ") "

		firstIncludeProto = True
		for proto in include_protos:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstIncludeProto: 
					queryString += "AND ("
					firstIncludeProto = False
				else:
					queryString += "OR "
			queryString += "%s = %d " % (common.COL_PROTO, proto)
		if not firstIncludeProto:
			queryString += ") "

		firstExcludeProto = True
		for proto in exclude_protos:
			if not isWhere:
				queryString += "WHERE " 
				isWhere = True
			else:
				if firstExcludeProto: 
					queryString += "AND ("
					firstExcludeProto = False
				else:
					queryString += "AND "
			queryString += "%s != %d " % (common.COL_PROTO, proto)
		if not firstExcludeProto:
			queryString += ") "

		# build the aggregation keys here
		doIPAddress = False
		doPort = False
		fieldList = ""
		if aggregate != None and aggregate != []:
			# aggregate all fields
			for field in aggregate:
				if fieldList != "":
					fieldList += ","
				if black_others:
					# black SQL magic. Select only the values of srcIP, dstIP that match the include_ips fields
					# (same with scrPort,dstPorts). Set all other values to 0
					includeList = None
					if field == common.COL_SRC_IP or field == common.COL_DST_IP:
						includeList = include_ips
					elif field == common.COL_SRC_PORT or field == common.COL_DST_PORT:
						includeList = include_ports
					if includeList:
						fieldString = "CASE " + field + " "
						for includeField in includeList:
							fieldString += " WHEN " + str(includeField) + " THEN " + field
						fieldList += fieldString + " ELSE 0 END as " + field
					else:
						fieldList += "MIN(" + field +  ") "
				else:
					# just take the field
					if field == common.COL_IPADDRESS:
						doIPAddress = True
					elif field == common.COL_PORT:
						doPort = True	
					else:
						fieldList += field 
			for field in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				fieldList += ",SUM(" + field + ") as " + field
				for p in common.AVAILABLE_PROTOS:
					fieldList += ",SUM(" + p + "_" + field + ") as " + p + "_" + field
		elif fields != None: 
			for field in fields:
				if field in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
						if fieldList != "":
							fieldList += ","
						fieldList += field + "_" + s
				else:
					if fieldList != "":
						fieldList += ","
					fieldList += field
			if not common.COL_BUCKET in fields:
				fieldList += "," + common.COL_BUCKET
		else:
			fieldList = "*"

		if doIPAddress and doPort:
			# we cannot have both of them. discard this request
			raise Exception("Logical error: doIPAddress and doPorts have been both set")

		if doIPAddress or doPort:
			if doIPAddress:
				bothDirection = common.COL_IPADDRESS
				srcDirection = common.COL_SRC_IP
				dstDirection = common.COL_DST_IP
			else:
				bothDirection = common.COL_PORT
				srcDirection = common.COL_SRC_PORT
				dstDirection = common.COL_DST_PORT
			# we need to merge srcIP and dstIPs
			srcQuery = "SELECT " + srcDirection + " as " + bothDirection + " %s FROM %s %s GROUP BY %s " % (fieldList, collectionName, queryString, srcDirection)
			dstQuery = "SELECT " + dstDirection + " as " + bothDirection + " %s FROM %s %s GROUP BY %s " % (fieldList, collectionName, queryString, dstDirection)
			addList = ""
			for field in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				addList += ",(a." + field + " + b." + field +") as " + field
				for p in common.AVAILABLE_PROTOS:
					addList += ",(a." + p + "_" + field + "+b." + p + "_" + field + ") as " + p + "_" + field

			queryString = "SELECT a.%s as %s%s FROM ((%s) a JOIN (%s) b ON a.%s = b.%s) " % (bothDirection, common.COL_ID, addList, srcQuery, dstQuery, bothDirection, bothDirection)
		else:
			queryString = "SELECT %s FROM %s %s " % (fieldList, collectionName, queryString)


		if aggregate and (not doIPAddress and not doPort):
			queryString += "GROUP BY "
			firstField = True
			for field in aggregate:
				if not firstField:
					queryString += ","
				else:
					firstField = False
				queryString += field + " "

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
		self.execute(queryString)
		queryResult =  self.cursor.fetchall()
		print "MySQL: Encoding Query ..."
		result = []
		total = dict()

		# TODO: Performance. Mysql returns Decimals instead of standard ints when aggregating
		#       While this is not a problem in itself, ujson cannot encode this
		#       and the result cannot be transmitted to the browser. We therefore do conversions
		#       to int, which hurts performance. We should either do
		#       1.) use MySQLDb.converters.conversions and change the mapping dict
		#       2.) fix ujson
		# 	Instead we convert it while encoding ...

		for row in queryResult:
			resultDoc = dict()
			isTotal = False
			for field in row:
				if field == common.COL_ID:
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
						resultDoc[fieldParts[0]][fieldParts[1]] = int(row[field])
					elif len(fieldParts) == 3:
						# TODO: not implemented ... 
						pass
					else:
						raise Exception("Internal Programming Error!")
				else:
					resultDoc[field] = int(row[field])
			if isTotal:
				total = resultDoc
			else:
				result.append(resultDoc)

		print "Got Results: ", len(result)
		#print "Total: ", total
		#print "Result: ", result
		#for r in result: 
		#	print r
		return (result, total)

	def run_query(self, collectionName, query):
		finalQuery = query % (collectionName)
		self.execute(finalQuery)
		return self.cursor.fetchall()


