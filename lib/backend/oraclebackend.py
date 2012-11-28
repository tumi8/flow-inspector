from sqlbasebackend import SQLBaseBackend
import sys
import config
import common
import time
import operator


class OracleBackend(SQLBaseBackend):
	def __init__(self, host, port, user, password, databaseName):
		SQLBaseBackend.__init__(self, host, port, user, password, databaseName)
		self.column_map = common.ORACLE_COLUMNMAP
		self.type_map = common.ORACLE_TYPE_MAPPER

	def connect(self):
		import cx_Oracle
		print "Oracle: Attempting new connect:" 
		try:
			connection_string = self.user + "/" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.databaseName
			print connection_string
			if self.cursor:
				self.cursor.close()
			if self.conn:
				self.conn.close()
			self.conn = cx_Oracle.Connection(connection_string)

			self.cursor = cx_Oracle.Cursor(self.conn)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Oracle database: ", inst 
			sys.exit(1)


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


	def handle_exception(self, exception):
		# try simply to reconnect and go again
		# TODO: improve handling
		print "Received exception: ", exception
		try:
			error, = exception.args
			#message = exception.args.message
		except Exception as e:
			# unknown error!
			print >> sys.stderr, "Received unknown exception in oracle backend that could not be unpacked: ", e
			sys.exit(-1)
		if error.code == 955:
			# index alreday exists. that's good. don't do anything
			print "Index already exists!"
			return False
		sys.exit(-1)
		self.connect()
		print "Trying to reconnet ..."
		return True

	def add_limit_to_string(self, string, limit):
		return "SELECT * FROM (" + string + ") WHERE ROWNUM <= " + str(limit)
