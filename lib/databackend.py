"""
Flow Inspector maintains a number of information that are different from flow data.
This information includes the system configuration, or analysis results of background
data. 
Because a user might decide to store his flows in some kind of specialized flow database,
another backend is necessary if the flow backend does not support data other than flows
(this can for example happen if the nfdump or SiLK tools are used to manage the flow
information).
Supported backends:
	- mysql
	- oracle

Author: Lothar Braun 
"""

import sys

class Backend:
	def __init__(self, host, port, user, password, databaseName):
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.databaseName = databaseName

	def connect(self):
		pass

	def prepareCollection(self, name, fieldDict):
		pass

	def query(self, collectionName, string):
		pass

	def insert(self, collectionName, fieldDict):
		pass

class MySQLBackend(Backend):
	def __init__(self, host, port, user, password, databaseName):
		import MySQLdb
		Backend.__init__(self, host, port, user, password, databaseName)

	def connect(self):
		import MySQLdb
		import _mysql_exceptions
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
			self.cursor.execute(string)
		except (AttributeError, MySQLdb.OperationalError):
			self.connect()
			self.execute(string)

	def executemany(self, string, objects):
		import MySQLdb
		import _mysql_exceptions
		try:
			self.cursor.executemany(string, objects)
		except (AttributeError, MySQLdb.OperationalError):
			self.connect()
			self.executemany(strin, objects)


	def prepareCollection(self, name, fieldDict):
		createString = "CREATE TABLE IF NOT EXISTS " + name + " ("
		first = True
		primary = ""
		for field in fieldDict:
			if not first:
				createString += ","
			createString += field + " " + fieldDict[field][0]
			if fieldDict[field][1] != None:
				primary = " PRIMARY KEY(" + field + ")"
			first = False
		if primary != "":
			createString += "," + primary
		createString += ")"
		self.execute(createString)


	def query(self, tablename, string):
		string = string % (tableName)
		self.execute(string)
		return self.cursor.fetchall()

	def insert(self, collectionName, fieldDict):
		queryString = "INSERT INTO " + collectionName + " ("
		typeString = ""
		valueString = ""
		updateString = ""
		for field in fieldDict:
			if typeString != "":
				typeString += ","
			if valueString != "":
				valueString += ","
			if updateString != "":
				updateString += ","
			updateString += field + "=" "VALUES(" + field + ")"
			typeString += field
			valueString += str(fieldDict[field])

		queryString += typeString + ") VALUES (" + valueString + ") ON DUPLICATE KEY UPDATE " + updateString

		self.execute(queryString)

class OracleBackend(Backend):
	def __init__(self, host, port, user, password, databaseName):
		Backend.__init__(self, host, port, user, password, databaseName)
		self.doCache = False
		self.connect()

	def connect(self):
		import cx_Oracle
		try:
			connection_string = self.user + "/" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.databaseName
			self.conn = cx_Oracle.Connection(connection_string)
			self.cursor = cx_Oracle.Cursor(self.conn)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Oracle database: ", inst 
			#sys.exit(1)


	def execute(self, string, params = None):
		import cx_Oracle
		try: 
			if params == None:
				self.cursor.execute(string)
			else:
				print string, params
				self.cursor.execute(string, params)
			self.conn.commit()
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
				print "DataBackend: Have seen unknown error. Terminating!"
				sys.exit(-1)

	def prepareCollection(self, name, fieldDict):
		createString = "CREATE TABLE  " + name + " ("
		first = True
		primary = ""
		for field in fieldDict:
			if not first:
				createString += ","
			createString += field + " " + fieldDict[field][0]
			if fieldDict[field][1] != None:
				primary = " PRIMARY KEY(" + field + ")"
			first = False
		if primary != "":
			createString += "," + primary
		createString += ")"
		self.execute(createString)


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



def getBackendObject(backend, host, port, user, password, databaseName):
	if backend == "mysql":
		return MySQLBackend(host, port, user, password, databaseName)
	elif backend == "oracle":
		return OracleBackend(host, port, user, password, databaseName)
	else:
		raise Exception("Data backend " + backend + " is not a supported backend")
