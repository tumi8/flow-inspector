from sqlbasebackend import SQLBaseBackend

import sys
import config
import common
import time
import operator

class MysqlBackend(SQLBaseBackend):
	def __init__(self, host, port, user, password, databaseName):
		SQLBaseBackend.__init__(self, host, port, user, password, databaseName)
		self.column_map = None
		self.type_map = common.MYSQL_TYPE_MAPPER


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
			self.cursor = self.conn.cursor()
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to MySQL database: ", inst 
			sys.exit(1)

	def insert(self, collectionName, fieldDict):
		updateString = ""
		typeString = ""
		cacheLine = () 
		valueString = ""
		for field in fieldDict:
			if typeString != "":
				typeString += ","
			if valueString != "":
				valueString += ","

			fieldValue = fieldDict[field][0]
			actionType = fieldDict[field][1]

			typeString += field
			if actionType == None or actionType == "ADD":
				if updateString != "":
					updateString += ","
				updateString += field + "=" + field + " + VALUES(" + field + ")"
			cacheLine = cacheLine + (fieldValue,)
			valueString += "%s"
	
		queryString = "INSERT INTO " + collectionName + " (" + typeString + ") VALUES (" + valueString + ") ON DUPLICATE KEY UPDATE " + updateString

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

	def handle_exception(self, exception):
		print "Received exception: ", exception
		try:
			(error,message) = exception
		except Exception as e:
			# unknown error!
			print >> sys.stderr, "Received unknown exception in mysql backend that could not be unpacked: ", exception
			sys.exit(-1)
		if error == 1051:
			# trying to delete unkonwn table. this is ok
			return False
		if error == 1050:
			# table already exists. that is ok
			return False
		if error == 1061:
			# index already exists. that is ok
			return False

		# try to reconnect
		# TODO: Implement better handling
		self.connect()
		return True
	
	def add_limit_to_string(self, string, limit):
		return string + " LIMIT " + str(limit)
