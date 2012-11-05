from flowbackend import Backend
import sys
import config
import common
import time
import operator


class OracleBackend(Backend):
	def __init__(self, host, port, user, password, databaseName):
		Backend.__init__(self, host, port, user, password, databaseName)
		self.tableInsertCache = dict()
		self.cachingThreshold = 100000
		self.counter = 0

		self.doCache = True

		self.conn = None
		self.cursor = None
		self.connect()

		self.executeTimes = 0
		self.executeManyTimes = 0
		self.executeManyObjects = 0

		self.tableNames = [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]
		for s in config.flow_bucket_sizes:
			self.tableNames.append(common.DB_FLOW_PREFIX + str(s))
			self.tableNames.append(common.DB_FLOW_AGGR_PREFIX + str(s))
			for index in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
				self.tableNames.append(index + "_" + str(s))

	def connect(self):
		import cx_Oracle
		print "Oracle: Attempting new connect:" 
		try:
			connection_string = self.user + "/" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.databaseName
			if self.cursor:
				self.cursor.close()
			if self.conn:
				self.conn.close()
			self.conn = cx_Oracle.Connection(connection_string)

			self.cursor = cx_Oracle.Cursor(self.conn)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Oracle database: ", inst 
			sys.exit(1)


	def execute(self, string, params = None):
		self.executeTimes += 1
		print "Execute: ", self.executeTimes
		maxtime = 10

		import cx_Oracle
		print string, params
		try: 
			start_time = time.time()
			if params == None:
				self.cursor.execute(string)
			else:
				self.cursor.execute(string, params)
			end_time = time.time()
			if end_time - start_time > maxtime:
				print "Execute: execute time was ", end_time - start_time

			start_time = time.time()
			self.conn.commit()
			end_time = time.time()
			if end_time - start_time > maxtime:
				print "Execute: commit time was ", end_time - start_time

		except (AttributeError, cx_Oracle.OperationalError) as e:
			print e
			self.connect()
			self.execute(string)
		except cx_Oracle.DatabaseError as e:
			print e 
			error, = e.args
			if error.code == 955:
				print "Table already exists!"
			else:
				print e
				print "FlowBackend: Have seen unknown error. Terminating!"
				sys.exit(-1)


	def executemany(self, string, objects, table = None):
		self.executeManyTimes += 1
		self.executeManyObjects += len(objects)
		print "Table: ", table, " ExecuteMany: ", self.executeManyTimes, " Current Objects: ", len(objects), "Total Objects: ", self.executeManyObjects
		maxtime = 5 

		import cx_Oracle
		try:
			#print "starting execute ..."
			start_time = time.time()
			self.cursor.executemany(string, objects)
			end_time = time.time()
			if end_time - start_time > maxtime:
				print "ExecuteMany: executemany time was ", end_time - start_time
			#print "ending execute ..."
			start_time = time.time()
			#print "starting commit ..."
			self.conn.commit()
			#print "ending commit ..."
			end_time = time.time()
			if end_time - start_time > maxtime:
				print "ExecuteMany: commit time was ", end_time - start_time
		except (AttributeError, cx_Oracle.OperationalError):
			self.connect()
			self.executemany(string, objects)

	def query(self, tablename, string):
		string = string % (tableName)
		self.execute(string)
		return self.cursor.fetchall()

	def insert(self, collectionName, fieldDict):
		queryString = "MERGE INTO " + collectionName + " target USING (SELECT "
		selectString = ""
		matchedString = ""
		notMatchedInsert = ""
		notMatchedValues = ""
		primary = ""
		params = {}
		for field in fieldDict:
			if selectString != "":
				selectString += ","
			if notMatchedInsert != "":
				notMatchedInsert += ","
			if notMatchedValues != "":
				notMatchedValues += ","
			selectString += ":"+field  + " as " + field
			params[field] = str(fieldDict[field][0])
			if fieldDict[field][1] != "PRIMARY":
				if matchedString != "":
					matchedString += ","
				if fieldDict[field][1] == None or fieldDict[field][1] == "ADD":
						matchedString += field + "=" + "SOURCE." + field + "+" + "target." + field
				elif fieldDict[field][1] == "UPDATE":
						matchedString += field + "=" + "target." + field
				elif fieldDict[field][1] == "KEEP":
						matchedString += field + "=" + "SOURCE." + field

			notMatchedInsert += "target." + field
			notMatchedValues += "SOURCE." + field
			if fieldDict[field][1] == "PRIMARY":
				if primary != "":
					primary += " AND "
				primary += "target." + field + "=SOURCE." + field
		
		queryString += selectString + " FROM dual) SOURCE ON (" + primary + ")"
		queryString += "WHEN MATCHED THEN UPDATE SET " + matchedString
		queryString += " WHEN NOT MATCHED THEN INSERT (" + notMatchedInsert + ") VALUES (" + notMatchedValues + ")" 
		if self.doCache:
			numElem = 1
			if collectionName in self.tableInsertCache:
				cache = self.tableInsertCache[collectionName][0]
				numElem = self.tableInsertCache[collectionName][1] + 1
				if queryString in cache:
					cache[queryString].append(params)
				else:
					cache[queryString] = [ params ]
			else:
				cache = dict()
				cache[queryString] = [ params ]
		
			self.tableInsertCache[collectionName] = (cache, numElem)

			self.counter += 1
			#if self.counter % 100000 == 0:
				#print "Total len:",  len(self.tableInsertCache)
				#for c in self.tableInsertCache:
					#print c, len(self.tableInsertCache[c][0]), self.tableInsertCache[c][1]
			
			if numElem > self.cachingThreshold:
				self.flushCache(collectionName)
		else:
			self.execute(queryString, params)

	def clearDatabase(self):
		import cx_Oracle
		for table in self.tableNames:
			try:
				self.cursor.execute("DROP TABLE " + table)
			except cx_Oracle.DatabaseError as e:
				error, = e.args
				if error.code == 942:
					print "Table " + table + " does not exist"
			except Exception as e:
				print "Have seen unknown error:", e
				print "Terminating!"
				sys.exit(-1)

	def prepareCollections(self):
		# we need to create several tables that contain flows and
		# pre aggregated flows, as well as tables for indexes that 
		# are precalculated on the complete data set. 

		# tables for storing bucketized flow data
		for s in config.flow_bucket_sizes:
			primary = "bucket"
			createString = "CREATE TABLE "
			createString += common.DB_FLOW_PREFIX + str(s) + " (" + common.COL_BUCKET + " NUMBER(10) NOT NULL"
			for v in config.flow_aggr_values:
				primary += "," + v
				createString += ", %s %s NOT NULL" % (v, common.ORACLE_TYPE_MAPPER[v])
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, common.ORACLE_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.ORACLE_TYPE_MAPPER[s])
			createString += ", PRIMARY KEY(" + primary + "))"
			self.execute(createString)

		# tables for storing aggregated timebased data
		for s in config.flow_bucket_sizes:
			createString = "CREATE TABLE "
			createString += common.DB_FLOW_AGGR_PREFIX + str(s) + " (" + common.COL_BUCKET + " NUMBER(10) NOT NULL"
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, common.ORACLE_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.ORACLE_TYPE_MAPPER[s])
			createString += ", PRIMARY KEY(" + common.COL_BUCKET + "))"
			self.execute(createString)
		
		# create precomputed index that describe the whole data set
		for table in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
			createString = "CREATE TABLE %s (%s NUMBER(10) NOT NULL" % (table, common.COL_ID)
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, common.ORACLE_TYPE_MAPPER[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.ORACLE_TYPE_MAPPER[s])
			for direction in [ "src", "dst" ]:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
					createString += ", %s %s DEFAULT 0" % (direction + "_" + s, common.ORACLE_TYPE_MAPPER[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
						createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, common.ORACLE_TYPE_MAPPER[s])

			createString += ", PRIMARY KEY(%s))" % (common.COL_ID)
			self.execute(createString)

		# create tables for storing incremental indexes
		for bucket_size in config.flow_bucket_sizes:
			for index in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
				table = index + "_" + str(bucket_size)
				createString = "CREATE TABLE " + table + " (" + common.COL_ID + " NUMBER(10) NOT NULL, " + common.COL_BUCKET + " NUMBER(10) NOT NULL"
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
					createString += ", %s %s DEFAULT 0" % (s, common.ORACLE_TYPE_MAPPER[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
						createString += ", %s %s DEFAULT 0" % (proto + "_" + s, common.ORACLE_TYPE_MAPPER[s])
				for direction in [ "src", "dst" ]:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS]:
						createString += ", %s %s DEFAULT 0" % (direction + "_" + s, common.ORACLE_TYPE_MAPPER[s])
					for proto in common.AVAILABLE_PROTOS:
						for s in config.flow_aggr_sums + [common.COL_FLOWS ]: 
							createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, common.ORACLE_TYPE_MAPPER[s])
	
				createString += ", PRIMARY KEY(" + common.COL_ID + "," + common.COL_BUCKET + "))"
				self.execute(createString)


	def createIndex(self, tableName, fieldName):
		try:
			self.execute("CREATE INDEX %s on %s (%s)" % (fieldName, tableName, fieldName))
		except:
			# most common cause: index does already exists
			# TODO check for errors
			pass

		
	def update(self, collectionName, statement, document, insertIfNotExists):
		fieldDict = {}
		for s in statement:
			if collectionName.startswith("index"):
				if s == common.COL_ID:
					# special handling for total field
					if statement[s] == "total":
						fieldDict[common.COL_ID] = (0, "PRIMARY")
					else:
						fieldDict[common.COL_ID] = (statement[s]["key"], "PRIMARY")
				if s == common.COL_BUCKET:
					if not "$set" in document:
						document["$set"] = {}
					document["$set"][common.COL_BUCKET] = statement[s][common.COL_BUCKET]
			

		for part in [ "$set", "$inc" ]:
			if not part in document:
				continue
			for v in document[part]:
				newV = v.replace('.', '_')
				if part == "$set":
					fieldDict[newV] = (document[part][v], "PRIMARY")
				else:
					fieldDict[newV] = (document[part][v], None)
		self.insert(collectionName, fieldDict)

	def flushCache(self, collectionName=None):
		print "flushing cache ..."
		if collectionName:
			cache = self.tableInsertCache[collectionName][0]
			for queryString in cache:
				cacheLines = cache[queryString]
				self.executemany(queryString, cacheLines, collectionName)
			del self.tableInsertCache[collectionName]
		else:
			# flush all collections
			while len( self.tableInsertCache) > 0:
				collection = self.tableInsertCache.keys()[0]
				self.flushCache(collection)
		print "flushed cache!"

	def getMinBucket(self, bucketSize = None):
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		tableName = common.DB_FLOW_PREFIX + str(bucketSize)
		self.execute("SELECT MIN(" + common.COL_BUCKET + ") as " + common.COL_BUCKET + " FROM %s" % (tableName))
		return self.cursor.fetchall()[0][0]
		
	def getMaxBucket(self, bucketSize = None):
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		tableName = common.DB_FLOW_PREFIX + str(bucketSize)
		self.execute("SELECT MAX(" + common.COL_BUCKET + ") as " + common.COL_BUCKET + " FROM %s" % (tableName))
		return self.cursor.fetchall()[0][0]

	def getBucketSize(self, startTime, endTime, resolution):
		for i,s in enumerate(config.flow_bucket_sizes):
			if i == len(config.flow_bucket_sizes)-1:
				return s
				
			tableName = common.DB_FLOW_AGGR_PREFIX + str(s)
			queryString = ("SELECT * FROM (SELECT " + common.COL_BUCKET + " FROM %s WHERE " + common.COL_BUCKET + " >= %d AND " + common.COL_BUCKET + " <= %d ORDER BY "+ common.COL_BUCKET + " ASC) where ROWNUM <= 1") % (tableName, startTime, endTime)
			self.execute(queryString);
			tmp = self.cursor.fetchall()
			minBucket = None
			if len(tmp) > 0:
				minBucket = tmp[0][0]

			queryString = ("SELECT * FROM (SELECT " + common.COL_BUCKET + " FROM %s WHERE " + common.COL_BUCKET + " >= %d AND " + common.COL_BUCKET + " <= %d ORDER BY " + common.COL_BUCKET + " DESC) WHERE ROWNUM <= 1") % (tableName, startTime, endTime)
			self.execute(queryString);
			tmp = self.cursor.fetchall()
			maxBucket = None
			if len(tmp) > 0:
				maxBucket = tmp[0][0]
			
			if not minBucket or not maxBucket:
				return s
			
			numSlots = (maxBucket-minBucket) / s + 1
			if numSlots <= resolution:
				return s

	def bucket_query(self, collectionName,  query_params):
		print "Oracle: Constructing query ... "
		min_bucket = self.getMinBucket();
		max_bucket = self.getMaxBucket();
		(result, total) = self.sql_query(collectionName, query_params)
		print "Oracle: Returning data to app ..."
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
			queryString += "%s = %d " % (common.COL_PROTO, common.getValueFromProto(proto))
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
			queryString += "%s != %d " % (common.COL_PROTO, common.getValueFromProto(proto))
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
			queryString = "SELECT * from (" + queryString + ") WHERE ROWNUM <=" + str(limit)

		print "Oracle: Running Query ..."
		self.execute(queryString)
		queryResult =  self.cursor.fetchall()
		print "Oracle: Encoding Query ..."
		result = []
		total = dict()

		columns = [i[0] for i in self.cursor.description]

		for row in queryResult:
			resultDoc = dict()
			isTotal = False
			idx = 0
			for field in columns:
				if field in common.ORACLE_COLUMNMAP:
					field = common.ORACLE_COLUMNMAP[field]
				fieldValue = row[idx]
				if field == common.COL_ID:
					if fieldValue == 0:
						isTotal = True
					fieldParts = []
				else: 
					tmp = field.split('_')
					fieldParts = []
					for f in tmp:
						if f in common.ORACLE_COLUMNMAP:
							fieldParts.append(common.ORACLE_COLUMNMAP[f])
						else:
							fieldParts.append(f)
						
				if len(fieldParts) > 1:
					# maximum depth is 3 
					# TODO: hacky ... 
					if len(fieldParts) == 2:
						if not fieldParts[0] in resultDoc:
							resultDoc[fieldParts[0]] = dict()
						resultDoc[fieldParts[0]][fieldParts[1]] = int(fieldValue)
					elif len(fieldParts) == 3:
						# TODO: not implemented ... 
						pass
					else:
						raise Exception("Internal Programming Error!")
				else:
					resultDoc[field] = int(fieldValue)
				idx += 1
			if isTotal:
				total = resultDoc
			else:
				result.append(resultDoc)

		#print "Got Results: ", len(result)
		#print "Total: ", total
		#print "Result: ", result
		#for r in result: 
		#	print r
		return (result, total)

	def run_query(self, collectionName, query):
		finalQuery = query % (collectionName)
		self.execute(finalQuery)
		return self.cursor.fetchall()


