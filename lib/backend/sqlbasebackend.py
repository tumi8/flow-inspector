"""
Base class for all sql-based flow backends
"""

from flowbackend import Backend
import sys
import config
import common
import time
import operator

class SQLBaseBackend(Backend):
	def __init__(self, host, port, user, password, databaseName):
		Backend.__init__(self, host, port, user, password, databaseName)
		self.tableInsertCache = dict()
		self.cachingThreshold = 100000
		self.counter = 0

		self.column_map = None

		self.doCache = True

		self.conn = None
		self.cursor = None
		self.dictCursor = None
		self.connect()

		self.dbType = None

		self.executeTimes = 0
		self.executeManyTimes = 0
		self.executeManyObjects = 0

		self.tableNames = [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]
		for s in config.flow_bucket_sizes:
			self.tableNames.append(common.DB_FLOW_PREFIX + str(s))
			self.tableNames.append(common.DB_FLOW_AGGR_PREFIX + str(s))
			for index in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
				self.tableNames.append(index + "_" + str(s))

	def handle_exception(self, exception):
		pass

	def insert(self, collectionName, fieldDict):
		pass

	def execute(self, string, params = None, cursor=None):
		self.executeTimes += 1
		#print "Execute: ", self.executeTimes
		maxtime = 10

		if cursor == None:
			cursor = self.cursor

		try: 
			start_time = time.time()
			if params == None:
				cursor.execute(string)
			else:
				cursor.execute(string, params)
			end_time = time.time()
			if end_time - start_time > maxtime:
				print "Execute: execute time was ", end_time - start_time

			start_time = time.time()
			self.conn.commit()
			end_time = time.time()
			if end_time - start_time > maxtime:
				print "Execute: commit time was ", end_time - start_time

		except Exception as e:
			if self.handle_exception(e):
				self.execute(string, params)


	def executemany(self, string, objects, table = None):
		self.executeManyTimes += 1
		self.executeManyObjects += len(objects)
		print "Table: ", table, " ExecuteMany: ", self.executeManyTimes, " Current Objects: ", len(objects), "Total Objects: ", self.executeManyObjects
		maxtime = 5 

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
		except Exception as e:
			if self.handle_exception(e):
				self.executemany(string, objects, table)

	def query(self, tablename, string):
		string = string % (tableName)
		self.execute(string)
		return self.cursor.fetchall()

	def clearDatabase(self):
		for table in self.tableNames:
			try:
				self.cursor.execute("DROP TABLE " + table)
			except Exception as e:
				if self.handle_exception(e):
					self.clearDatabase()

	def createIndex(self, tableName, fieldName):
		try:
			self.execute("CREATE INDEX %s on %s (%s)" % (fieldName, tableName, fieldName))
		except Exception as e:
			if self.handle_exception(e):
				self.createIndex(tableName, fieldName)
			

		
	def update(self, collectionName, statement, document, insertIfNotExists, comes_from_cache = False):
		if collectionName.startswith("index") and not comes_from_cache:
			self.handle_index_update(collectionName, statement, document, insertIfNotExists)
			return

		fieldDict = {}
		for s in statement:
			# TODO: this will break if we have a non-flow collection name that 
			#       has a name starting with index ... 
			#       This is a new problem that occurred when we decided to use 
			#       the flowbackend classes for storing other kinds of data ...
			#       Think about how to solve this issue
			if collectionName.startswith("index"):
				if s == common.COL_ID:
					# special handling for total field
					if statement[s] == "total":
						fieldDict[common.COL_ID] = (0, "PRIMARY")
					else:
						fieldDict[common.COL_ID] = (statement[s], "PRIMARY")
				if s == common.COL_BUCKET:
					if not "$set" in document:
						document["$set"] = {}
					document["$set"][common.COL_BUCKET] = statement[s]
			elif not collectionName.startswith("flows"):
				if s == "_id":
					fieldDict["id"] = (statement[s], "PRIMARY")
				else:
					fieldDict[s] = (statement[s], "PRIMARY")

			

		for part in [ "$set", "$inc" ]:
			if not part in document:
				continue
			for v in document[part]:
				newV = v.replace('.', '_')
				if part == "$set":
					fieldDict[newV] = (document[part][v], "SET")
				else:
					fieldDict[newV] = (document[part][v], None)
		self.insert(collectionName, fieldDict)

	def flushCache(self, collectionName=None):
		if collectionName == None:
			self.flush_index_cache()

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

	def getMinBucket(self, bucketSize = None):
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		tableName = common.DB_FLOW_PREFIX + str(bucketSize)
		self.execute("SELECT MIN(" + common.COL_BUCKET + ") as " + common.COL_BUCKET + " FROM %s" % (tableName))
		result =  self.cursor.fetchall()
		if common.COL_BUCKET in result[0]:
			return result[0][common.COL_BUCKET]
		else:
			return result[0][0]

		
	def getMaxBucket(self, bucketSize = None):
		if not bucketSize:
			# use minimal bucket size
			bucketSize = config.flow_bucket_sizes[0]
		tableName = common.DB_FLOW_PREFIX + str(bucketSize)
		self.execute("SELECT MAX(" + common.COL_BUCKET + ") as " + common.COL_BUCKET + " FROM %s" % (tableName))
		result =  self.cursor.fetchall()
		if common.COL_BUCKET in result[0]:
			return result[0][common.COL_BUCKET]
		else:
			return result[0][0]


	def add_limit_to_string(self, string, limit):
		pass

	def getBucketSize(self, startTime, endTime, resolution):
		for i,s in enumerate(config.flow_bucket_sizes):
			if i == len(config.flow_bucket_sizes)-1:
				return s
				
			tableName = common.DB_FLOW_AGGR_PREFIX + str(s)
			queryString = ("SELECT " + common.COL_BUCKET + " FROM %s WHERE " + common.COL_BUCKET + " >= %d AND " + common.COL_BUCKET + " <= %d ORDER BY "+ common.COL_BUCKET + " ASC") % (tableName, startTime, endTime)
			queryString = self.add_limit_to_string(queryString, 1)
			
			self.execute(queryString);
			tmp = self.cursor.fetchall()
			minBucket = None
			if len(tmp) > 0:
				minBucket = tmp[0][0]

			queryString = ("SELECT " + common.COL_BUCKET + " FROM %s WHERE " + common.COL_BUCKET + " >= %d AND " + common.COL_BUCKET + " <= %d ORDER BY " + common.COL_BUCKET + " DESC") % (tableName, startTime, endTime)
			queryString = self.add_limit_to_string(queryString, 1)

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
		print "SQL: Constructing query ... "
		min_bucket = self.getMinBucket();
		max_bucket = self.getMaxBucket();
		(result, total) = self.sql_query(collectionName, query_params)
		print "SQL: Returning data to app ..."
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
		print "include_protos:", include_protos
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
						fieldList += "MAX(" + field +  ") "
				else:
					# just take the field
					if field == common.COL_IPADDRESS:
						doIPAddress = True
					elif field == common.COL_PORT:
						doPort = True	
					else:
						fieldList += field 
			# take the fields that should not be aggregated and not summed. Take maximum
			if fields != None:
				for field in fields:
					if fieldList != "":
						fieldList += ","
					fieldList += "MAX(" + field + ") as " + field
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
				addList += ",SUM(" + field +") as " + field
				for p in common.AVAILABLE_PROTOS:
					addList += ",SUM(" + p + "_" + field + ") as " + p + "_" + field

			if self.type == "oracle": 
				queryString = "SELECT %s as %s%s FROM ((%s) UNION ALL (%s)) " % (bothDirection, common.COL_ID, addList, srcQuery, dstQuery)
			else:
				queryString = "SELECT %s as %s%s FROM ((%s) UNION (%s)) as T " % (bothDirection, common.COL_ID, addList, srcQuery, dstQuery)
				

		else:
			queryString = "SELECT %s FROM %s %s " % (fieldList, collectionName, queryString)


		if aggregate:
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
			queryString = self.add_limit_to_string(queryString, limit)

		print "SQL: Running Query ..."
		#print queryString
		self.execute(queryString)
		queryResult =  self.cursor.fetchall()
		print "SQL: Encoding Query ..."
		result = []
		total = dict()

		columns = [i[0] for i in self.cursor.description]

		for row in queryResult:
			resultDoc = dict()
			isTotal = False
			idx = 0
			for field in columns:
				if self.column_map and field in self.column_map:
					field = self.column_map[field]
				fieldValue = row[idx]
				if field == common.COL_ID:
					if fieldValue == 0:
						isTotal = True
					fieldParts = []
				else: 
					tmp = field.split('_')
					fieldParts = []
					for f in tmp:
						if self.column_map and f in self.column_map:
							fieldParts.append(self.column_map[f])
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

	def prepareCollections(self):
		# we need to create several tables that contain flows and
		# pre aggregated flows, as well as tables for indexes that 
		# are precalculated on the complete data set. 

		# tables for storing bucketized flow data
		for s in config.flow_bucket_sizes:
			primary = common.COL_BUCKET
			createString = "CREATE TABLE "
			createString += common.DB_FLOW_PREFIX + str(s) + " (" + common.COL_BUCKET + " " + self.type_map[common.COL_BUCKET] + " NOT NULL"
			for v in config.flow_aggr_values:
				primary += "," + v
				createString += ", %s %s NOT NULL" % (v, self.type_map[v])
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, self.type_map[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, self.type_map[s])
			createString += ", PRIMARY KEY(" + primary + "))"
			self.execute(createString)

		# tables for storing aggregated timebased data
		for s in config.flow_bucket_sizes:
			createString = "CREATE TABLE "
			createString += common.DB_FLOW_AGGR_PREFIX + str(s) + " (" + common.COL_BUCKET + " " + self.type_map[common.COL_BUCKET] + " NOT NULL"
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, self.type_map[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, self.type_map[s])
			createString += ", PRIMARY KEY(" + common.COL_BUCKET + "))"
			self.execute(createString)
		
		# create precomputed index that describe the whole data set
		for table in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
			createString = "CREATE TABLE %s (%s %s NOT NULL" % (table, common.COL_ID, self.type_map[common.COL_ID])
			for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
				createString += ", %s %s DEFAULT 0" % (s, self.type_map[s])
			for proto in common.AVAILABLE_PROTOS:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
					createString += ", %s %s DEFAULT 0" % (proto + "_" + s, self.type_map[s])
			for direction in [ "src", "dst" ]:
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
					createString += ", %s %s DEFAULT 0" % (direction + "_" + s, self.type_map[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
						createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, self.type_map[s])

			createString += ", PRIMARY KEY(%s))" % (common.COL_ID)
			self.execute(createString)

		# create tables for storing incremental indexes
		for bucket_size in config.flow_bucket_sizes:
			for index in [ common.DB_INDEX_NODES, common.DB_INDEX_PORTS ]:
				table = index + "_" + str(bucket_size)
				createString = "CREATE TABLE " + table + " (" + common.COL_ID + " " + self.type_map[common.COL_ID] + " NOT NULL, " + common.COL_BUCKET + " " + self.type_map[common.COL_BUCKET] + " NOT NULL"
				for s in config.flow_aggr_sums + [ common.COL_FLOWS ]:
					createString += ", %s %s DEFAULT 0" % (s, self.type_map[s])
				for proto in common.AVAILABLE_PROTOS:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS ]: 
						createString += ", %s %s DEFAULT 0" % (proto + "_" + s, self.type_map[s])
				for direction in [ "src", "dst" ]:
					for s in config.flow_aggr_sums + [ common.COL_FLOWS]:
						createString += ", %s %s DEFAULT 0" % (direction + "_" + s, self.type_map[s])
					for proto in common.AVAILABLE_PROTOS:
						for s in config.flow_aggr_sums + [common.COL_FLOWS ]: 
							createString += ", %s %s DEFAULT 0" % (direction + "_" + proto + "_" + s, self.type_map[s])
	
				createString += ", PRIMARY KEY(" + common.COL_ID + "," + common.COL_BUCKET + "))"
				self.execute(createString)


	def find(self, collectionName,  spec, fields=None, sort=None, limit=None):
		fieldsString = ""
		if fields == None:
			fieldsString = "*"
		else: 
			for f in fields:
				if fields[f] == 1:
					if fieldsString != "":
						fieldsString += ","
					fieldsString += f
			if fieldsString == "":
				fieldsString = "*"
		query = "SELECT " + fieldsString + " FROM " + collectionName + " "
		where_clause = ""
		order_clause = ""
		for field in spec:
			if where_clause != "":
				where_clause += " AND "
			else:
				where_clause = "WHERE "
			if field.startswith("$"):
				if field == "$or": 
					second_level_clause = ""
					for cond in spec[field]:
						for key in cond:
							if second_level_clause != "":
								second_level_clause += " OR "
							second_level_clause += key + '=' + "'" + cond[key] + "'"
					where_clause += " (" + second_level_clause + ") "
				else:
					raise Exception("Mongo operator " + field + " is not yet implemented!")
			else:
				if type(spec[field]) == type(dict()):
					for i in spec[field]:
						if i == "$lt":
							operator = "<"
						elif i == "$lte":
							operator = "<="
						elif i == "$gt":
							operator = ">"
						elif i == "$gte":
							operator = ">="
						else:
							raise Exception("Operator not implemented")
						operand = str(spec[field][i])
				else:
					operator = "="
					operand = str(spec[field])
				where_clause += field + operator +"'" + operand + "'"

		if sort != None:
			if len(sort) > 1:
				raise Exception("Can only sort for a single field!")
			elif len(sort) == 1:
				for field in sort:
					order_clause = " ORDER BY " + field + " "
					if sort[field] == 1:
						order_clause += "ASC"
					else:
						order_clause += "DESC"
		if where_clause != "":
			query += where_clause
		if order_clause != "":
			query += order_clause
		if limit:
			query = self.add_limit_to_string(query, limit)

		self.execute(query, None, self.dictCursor)
		if self.type == "oracle":
			# As alway: Things do not work with oracle ...
			typeWrapper = self.dynamic_type_wrapper[collectionName]
			rows = self.dictCursor.fetchall()
			if typeWrapper == None:
				print "ERROR: Unknown collection. Cannot create dictionary ..."
				return dict()
			columns = [typeWrapper[i[0]] for i in self.dictCursor.description]
			return [dict(zip(columns, row)) for row in rows]
		else:
			ret = self.dictCursor.fetchall()
		return ret
	
	def distinct(self, collectionName, field):
		d =  self.find(collectionName, [], {"DISTINCT " + field: 1})
		ret = []
		for entry in d:
			ret.append(entry[field])

		return ret
