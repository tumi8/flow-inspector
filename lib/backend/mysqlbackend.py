from sqlbasebackend import SQLBaseBackend

import sys
import config
import common
import time
import operator

class MysqlBackend(SQLBaseBackend):
	def __init__(self, host, port, user, password, databaseName, insertMode="UPDATE"):
		from warnings import filterwarnings
		import MySQLdb
		import _mysql_exceptions
		filterwarnings('ignore', category = MySQLdb.Warning)
		SQLBaseBackend.__init__(self, host, port, user, password, databaseName, insertMode)
		self.column_map = None
		self.type_map = common.MYSQL_TYPE_MAPPER
		self.type = "mysql"

	def getCollectionList(self):
		self.execute("SHOW TABLES")
		coll_list = []
		for i in  self.cursor.fetchall():
			coll_list.append(i[0])
		return coll_list

	def connect(self):
		import MySQLdb
		import _mysql_exceptions
		#print "Connecting ..."
		try:
			dns = dict(
				db = self.databaseName,
				host = self.host,
				port = self.port,
				user = self.user,
				passwd = self.password
			)
			if self.conn:
				self.conn.close()
			self.conn = MySQLdb.connect(**dns)
			self.cursor = self.conn.cursor()
			self.dictCursor = self.conn.cursor()
			#self.dictCursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to MySQL database: ", inst 
			sys.exit(1)

	def insert_insert(self, collectionName, fieldDict):
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
			cacheLine = cacheLine + (fieldValue,)
			valueString += "%s"
	
		queryString = "INSERT INTO " + collectionName + " (" + typeString + ") VALUES (" + valueString + ") "
		self.add_to_cache(collectionName, queryString, cacheLine)


	def insert_update(self, collectionName, fieldDict):
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
			elif actionType == "SET" or actionType == "PRIMARY":
				if updateString != "":
					updateString += ","
				updateString += field + "=VALUES(" + field + ")"
			cacheLine = cacheLine + (fieldValue,)
			valueString += "%s"
	
		queryString = "INSERT INTO " + collectionName + " (" + typeString + ") VALUES (" + valueString + ") "
		if updateString != "":
			queryString += " ON DUPLICATE KEY UPDATE " + updateString

		self.add_to_cache(collectionName, queryString, cacheLine)

	def handle_exception(self, exception):
		#print "Received exception: ", exception
		try:
			(error,message) = exception
		except Exception as e:
			# unknown error!
			print >> sys.stderr, "Received unknown exception in mysql backend that could not be unpacked: ", exception
			sys.exit(-1)
		if error == 1051:
			# trying to delete unkonwn table. this is ok
			return False
		elif error == 1050:
			# table already exists. that is ok
			return False
		elif error == 1061:
			# index already exists. that is ok
			return False

		elif error == 1054:
			# unknown column in string. This is likely to be a programming error, but we
			# need more context to understand what it is. Handle this condition in the
			# caller ...
			raise 

		elif error == 1146:
			# table does not exist. Probably means that no data was imported yet.
			return False
		elif error == 1064:
			# error in SQL syntax! This is a programming error and should result in the termination of 
			# the process or should be handled by another instance ...
			raise 
		elif error == 1072:
			# programming error: field not in table
			raise

		print "ERROR: Received exception (" + str(error) + "):", message
		return True
	
	def add_limit_to_string(self, string, limit):
		return string + " LIMIT " + str(limit)

	def prepareCollection(self, name, fieldDict):
		createString = "CREATE TABLE IF NOT EXISTS " + name + " ("
		first = True
		primary = ""
		indexes = ""
		table_options = ""
		for field in fieldDict:
			if field == "_id":
				fieldMod = "id"
			else:
				fieldMod = field

			if field == "table_options":
				table_options = fieldDict[field]
			elif fieldDict[field][0].endswith("INDEX"):
				if indexes != "":
					indexes += ","
				indexes += fieldDict[field][0] + " " + fieldMod + " (" + fieldDict[field][1] + ")" 
			else:
				if not first:
					createString += ","
				createString += fieldMod + " " + fieldDict[field][0]
				if fieldDict[field][1] == "PRIMARY":
					primary = " PRIMARY KEY(" + fieldMod + ")"
				if fieldDict[field][2] != None:
					createString += " " + fieldDict[field][2]
				first = False
		if primary != "":
			createString += "," + primary
		if indexes != "":
			createString += "," + indexes
		createString += ") " + table_options
		self.execute(createString)
	
	def getIndexes(self, collectionName):
		indexFields = []
		self.execute("SHOW INDEX FROM " + collectionName + ";")
		for indexField in self.cursor.fetchall():
			indexFields.append(indexField[4])
		return indexFields

	def check_index_column(self, column, collectionName):
		return column in self.getIndexes(collectionName)

	def get_table_sizes(self):
		self.execute("SHOW TABLE STATUS")
		ret = {}
		for entry in self.cursor.fetchall():
			ret[entry[0]] = entry[6]
		return ret

