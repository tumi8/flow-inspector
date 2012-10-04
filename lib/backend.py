"""
Backend wrappers. Select and create the appropriate objects for 
backend communication.

Currently supported backends:
	- mongodb
	- mysql

Author: Lothar Braun 
"""

import sys

class Collection:
	def __init__(self, backendObject, collectionName):
		self.backendObject = backendObject
		self.collectionName = collectionName
		self.name = collectionName

	def createIndex(self, fieldName):
		self.backendObject.createIndex(self.collectionName, fieldName)

	def update(self, statement, document, insertIfNotExist):
		self.backendObject.update(self.collectionName, statement, document, insertIfNotExist)

	def find(self, spec=None, fields=None):
		return self.backendObject.find(self.collectionName, spec, fields)

	def find_one(self, spec, fields=None, sort=None):
		return self.backendObject.find_one(self.collectionName, spec, fields, sort)

class Backend:
	def __init__(self, conn, backendType, databaseName):
		self.conn = conn
		self.backendType = backendType
		self.databaseName = databaseName


		if self.backendType == "mysql":
			self.cursor = self.conn.cursor()

	def clearDatabase(self):
		pass

	def getCollection(self, name):
		return Collection(self, name)
	
	def prepareCollections(self):
		pass

	def createIndex(self, collectionName, fieldName):
		pass

	def update(self, collectionName, statement, document, insertIfNotExists):
		pass

	def find(self, collectionName, spec, fields):
		pass

	def find_one(self, collectionName, spec, fields, sort):
		pass


class MongoBackend(Backend):
	def __init__(self, conn, backendType, databaseName):
		Backend.__init__(self, conn, backendType, databaseName)
		self.dst_db = self.conn[databaseName]
	
	def clearDatabase(self):
		self.conn.drop_database(self.databaseName)


	def createIndex(self, collectionName, fieldName):
		collection = self.dst_db[collectionName]
		collection.create_index(fieldName)

	def update(self, collectionName, statement, document, insertIfNotExists):
		collection = self.dst_db[collectionName]
		collection.update(statement, document, insertIfNotExists)

	def find(self, collectionName, spec, fields):
		collection = self.dst_db[collectionName]
		return collection.find(spec=spec, fields=fields)

	def find_one(self, collectionName, spec, fields, sort):
		collection = self.dst_db[collectionName]
		return collection.find_one(spec, fields=fields, sort=sort)



class MysqlBackend(Backend):
	def __init__(self, conn, backendType, databaseName):
		Backend.__init__(self, conn, backendType, databaseName)
		self.cursor = conn.cursor()
	
	def clearDatabase(self):
		self.cursor.execute("SHOW TABLES")	
		tables = self.cursor.fetchall()
		for table in tables:
			self.cursor.execute("DROP TABLE " + table[0])

	def prepareCollections(self):
		pass

	def createIndex(self, tableName, fieldName):
		pass
		
	def update(self, collectionName, statement, document, insertIfNotExists):
		pass

	def find(self, collectionName, spec, fields):
		pass


	def find_one(self, collectionName, spec, fields, sort):
		pass
	
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
