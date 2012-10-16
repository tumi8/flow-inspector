"""
Flow Inspector maintains a number of information that are different from flow data.
This information includes the system configuration, or analysis results of background
data. 
Because a user might decide to store his flows in some kind of specialized flow database,
another backend is necessary if the flow backend does not support data other than flows
(this can for example happen if the nfdump or SiLK tools are used to manage the flow
information).
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

	def connect(self):
		import cx_Oracle
		try:
			connection_string = self.user + "/" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.databaseName
			self.conn = cx_Oracle.Connection(connection_string)
			self.cursor = cx_Oracle.Cursor(conn)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Oracle database: ", inst 
			sys.exit(1)


	def execute(self, string):
		import cx_Oracle
		try: 
			self.cursor.execute(string)
		except (AttributeError, cx_Oracle.OperationalError):
			self.connect()
			self.execute(string)

#	def executemany(self, string, objects):
#		import cx_Oracle
#		try:
#			self.cursor.executemany(string, objects)
#		except (AttributeError, cx_Oracle.OperationalError):
#			self.connect()
#			self.executemany(strin, objects)


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
		queryString = "MERGE INTO " + collectionName + " target USING (SELECT "
		selectString = ""
		matchedString = ""
		notMatchedInsert = ""
		notMatchedValues = ""
		for field in fieldDict:
			if selectString != "":
				selectString += ","
			if matchedString != "":
				matchedString += ","
			if notMatchedInsert != "":
				notMatchedInsert += ","
			if notMatchedValues != "":
				notMatchedValues += ","
			selectString += str(fieldDict[field])  + " as " + field
			matchedString += field + "=" + str(fieldDict[field])
			notMatchedInsert += "target." + field
			notMatchedValues += "SOURCE." + field
		
		queryString += selectString + " FROM dual) SOURCE ON target
		
		self.execute(queryString)


def getBackendObject(backend, host, port, user, password, databaseName):
	if backend == "mysql":
		return MySQLBackend(host, port, user, password, databaseName)
	elif backend == "oracle":
		return OracleBackend(host, port, user, password, databaseName)
	else:
		raise Exception("Data backend " + backend + " is not a supported backend")
