from sqlbasebackend import SQLBaseBackend
import sys
import config
import common
import time
import operator


class OracleBackend(SQLBaseBackend):
	def __init__(self, host, port, user, password, databaseName, insertMode="UPDATE"):
		SQLBaseBackend.__init__(self, host, port, user, password, databaseName, insertMode)
		self.column_map = common.ORACLE_COLUMNMAP
		self.type_map = common.ORACLE_TYPE_MAPPER
		self.dynamic_type_wrapper = dict()
		self.type = "oracle"

	def getCollectionList(self):
		self.execute("SELECT table_name FROM user_tables")
		coll_list = []
		for i in  self.cursor.fetchall():
			coll_list.append(i[0])
		return coll_list

	def connect(self):
		import cx_Oracle
		try:
			connection_string = self.user + "/" + self.password + "@" + self.host + ":" + str(self.port) + "/" + self.databaseName
			if self.cursor:
				self.cursor.close()
			if self.conn:
				self.conn.close()
			self.conn = cx_Oracle.Connection(connection_string)

			self.cursor = cx_Oracle.Cursor(self.conn)
			self.dictCursor = cx_Oracle.Cursor(self.conn)
		except Exception as inst:
			print >> sys.stderr, "Cannot connect to Oracle database: ", inst,

	def insert_insert(self, collectionName, fieldDict):
		typeString = ""
		params = {}
		valueString = ""
		for field in fieldDict:
			if typeString != "":
				typeString += ","
			if valueString != "":
				valueString += ","

			fieldValue = fieldDict[field][0]

			typeString += field
			params[field] = str(fieldValue)
			valueString += ":" + field
	
		queryString = "INSERT INTO " + collectionName + " (" + typeString + ") VALUES (" + valueString + ") "
		self.add_to_cache(collectionName, queryString, params)


	def insert_update(self, collectionName, fieldDict):
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
				elif fieldDict[field][1] == "UPDATE" or fieldDict[field][1] == "SET":
						matchedString += "target." + field + "=" + "SOURCE." + field
				elif fieldDict[field][1] == "KEEP":
						matchedString += "target." + field + "=" + "target." + field

			notMatchedInsert += "target." + field
			notMatchedValues += "SOURCE." + field
			if fieldDict[field][1] == "PRIMARY":
				if primary != "":
					primary += " AND "
				primary += "target." + field + "=SOURCE." + field
		
		queryString += selectString + " FROM dual) SOURCE ON (" + primary + ")"
		queryString += " WHEN MATCHED THEN UPDATE SET " + matchedString
		queryString += " WHEN NOT MATCHED THEN INSERT (" + notMatchedInsert + ") VALUES (" + notMatchedValues + ")" 
		
		self.add_to_cache(collectionName, queryString, params)

	def clearDatabase(self):
		import cx_Oracle
		for table in self.tableNames:
			try:
				self.cursor.execute("DROP TABLE " + table)
			except cx_Oracle.DatabaseError as e:
				error, = e.args
				if error.code == 942:
					print >> sys.stderr, "Table " + table + " does not exist"
			except Exception as e:
				print >> sys.stderr, "Have seen unknown error:", e
				print >> sys.stderr, "Terminating!"
				sys.exit(-1)


	def handle_exception(self, exception):
		# try simply to reconnect and go again
		# TODO: improve handling
		try:
			error, = exception.args
			#message = exception.args.message
		except Exception as e:
			# unknown error!
			print >> sys.stderr, "Received unknown exception in oracle backend that could not be unpacked: ", e
			sys.exit(-1)
		if type(error) == type(str()):
			# this is not a DB exception. reraise
			raise exception
		if error.code == 955 or error.code == 1408:
			# index alreday exists. that's good. don't do anything
			return False
		print >> sys.stderr, "Exception on query:", self.last_query
		print >> sys.stderr, "Received exception: ", exception,
		sys.exit(1)
		self.connect()
		print "Trying to reconnet ..."
		return True

	def add_limit_to_string(self, string, limit):
		if type(limit) is int: 
			return "SELECT * FROM (" + string + ") WHERE ROWNUM <= " + str(limit)
		if type(limit) is tuple:
			return "SELECT * FROM (" + string + ") WHERE ROWNUM >= " + str(limit[0]) + " AND ROWNUM <= " + str(limit[1])

	def fillDynamicTypeWrapper(self, name, fieldDict):
		oracleNameTypeWrapper = dict()
		for field in fieldDict:
			if field == "_id":
				fieldMod = "id"
			else:
				fieldMod = field
			if field != "table_options" and not fieldDict[field][0].endswith("INDEX"):
				if field == "_id": 
					oracleNameTypeWrapper["ID"] = '_id'
				else:
					oracleNameTypeWrapper[fieldMod.upper()] = fieldMod
		self.dynamic_type_wrapper[name] = oracleNameTypeWrapper


	def prepareCollection(self, name, fieldDict):
		self.fillDynamicTypeWrapper(name, fieldDict)

		createString = "CREATE TABLE  " + name + " ("
		primary = ""
		indexes = ""
		first = True
		sequences = []
		triggers = []
		indexes = []
		for field in fieldDict:
			if field == "_id":
				fieldMod = "id"
			else:
				fieldMod = field

			if field == "table_options":
				pass
			elif fieldDict[field][0].endswith("INDEX"):
				index_create_string = "" 
				for f in fieldDict[field]:
					if not f.endswith("INDEX"):
						if index_create_string == "":
							index_create_string = "CREATE INDEX " + name + "_" + fieldMod + " ON " + name + " ("
						else:
							index_create_string += ","
						index_create_string += f
				index_create_string += ")"
				indexes.append(index_create_string) 
			else:
				if not first:
					createString += ","
				createString += fieldMod + " " + fieldDict[field][0]
				if fieldDict[field][1] == "PRIMARY":
					primary = " PRIMARY KEY(" + fieldMod + ")"
				if fieldDict[field][2] == "AUTO_INCREMENT":
					# Oracle does not know AUTO_INCREME?NTS. we need a trigger and sequence
					sequences.append("CREATE SEQUENCE " + name + "_" + fieldMod + '_seq START WITH 1 INCREMENT BY 1 NOMAXVALUE')
					triggers.append("CREATE OR REPLACE TRIGGER " + name + "_" + fieldMod + "_trigger BEFORE INSERT ON " + name + " REFERENCING NEW AS new FOR EACH ROW Begin SELECT " + name + "_" + fieldMod + "_seq.NEXTVAL INTO :new." + fieldMod + " FROM DUAL; END;")
				elif fieldDict[field][2] != None:
					createString += " " + fieldDict[field][2]
				
				first = False

		if primary != "":
			createString += "," + primary
		createString += ") " 
		self.execute(createString)
		for seq in sequences:
			self.execute(seq)
		for trigger in triggers:
			self.execute(trigger)
		for index in indexes:
			self.execute(index)


	def get_table_sizes(self):
		self.execute("""SELECT  table_name, sum(bytes) FROM  (SELECT segment_name table_name, bytes  FROM user_segments  WHERE segment_type = 'TABLE'  UNION ALL  SELECT i.table_name, s.bytes  FROM user_indexes i, user_segments s  WHERE s.segment_name = i.index_name  AND   s.segment_type = 'INDEX'  UNION ALL  SELECT l.table_name, s.bytes  FROM user_lobs l, user_segments s  WHERE s.segment_name = l.segment_name  AND   s.segment_type = 'LOBSEGMENT'  UNION ALL  SELECT l.table_name, s.bytes  FROM user_lobs l, user_segments s  WHERE s.segment_name = l.index_name  AND   s.segment_type = 'LOBINDEX') GROUP BY table_name  ORDER BY SUM(bytes) desc""")
	
		ret = {}
		for entry in self.cursor.fetchall():
			ret[entry[0]] = entry[1]
		return ret

