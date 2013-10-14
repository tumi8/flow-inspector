import common

import sys

########################## functions
def compareTables(a, b):
	compsA = a.split('_')
	compsB = b.split('_')
	return -cmp(int(compsA[1]), int(compsB[1])) or cmp(int(compsA[2]), int(compsB[2])) or cmp(int(compsA[3]), int(compsB[3]))


def ip2int(string):
	ip_fields=string.split('.')
	return 2**24*long(ip_fields[0])+2**16*long(ip_fields[1])+2**8*long(ip_fields[2])+long(ip_fields[3])

def getTableNameFromTimestamp(timestamp):
	timeObj = datetime.datetime.utcfromtimestamp(timestamp)
	if timeObj.minute < 30:
		halfHour = 0
	else:
		halfHour = 1
	tableName = "h_%0.4d%0.2d%0.2d_%0.2d_%0.1d" % (timeObj.year, timeObj.month, timeObj.day, timeObj.hour, halfHour)
	return tableName

def getTables(tables, firstTimestamp, lastTimestamp, TYPE):
	"""
		Expects a sorted list of table names (sorted by time).
		Returns the table which contains the first flow that 
		has a firstSwitched time of firstTimestamp
	"""
	if firstTimestamp == 0 and lastTimestamp == 0:
		return tables
	
	# restrict tablespace on first timestamp
	if firstTimestamp != 0:
		firstTableName = getTableNameFromTimestamp(firstTimestamp)

		try:
			idx = tables.index(firstTableName) 
		except:
			# no such table in list
			return []
		tables =  tables[idx:len(tables)]
	if lastTimestamp != 0:
		lastTable = getTableNameFromTimestamp(lastTimestamp)
		try:
			idx = tables.index(lastTable)
		except:
			# no such table in list
			return tables
		tables = tables[0:idx]
	return tables
 

############################## Classes

class BaseImporter:
	def __init__(self, args):
		self.args = args

	def get_next_flow(self):
		raise Exception("Derived class did not implement get_next_flow!")

	def get_db_connection(self):
		# check if is there a MySQL or a PostgreSQL database
		try:
			print "Trying postgresql database ..."
			import psycopg2
			self.TYPE = "postgresql"
			dns = dict(
				database = self.args.src_database, 
				host = self.args.src_host,
				user = self.args.src_user,
				password = self.args.src_password
			)
			if self.args.src_port is not None:
				dns['port'] = self.args.src_port
			self.conn = psycopg2.connect(**dns)
			self.c = self.conn.cursor()
			
			print "Successfully connected to postgresql db ..."
		except Exception, e:
			try:
				print "Failed to connect to postgresql db. Reason: ", e
				print "Trying mysql instead ..."
				#import MySQLdb
				#import _mysql_exceptions
		
				self.TYPE = "mysql"
				dns = dict(
					db = self.args.src_database, 
					host = self.args.src_host,
					user = self.args.src_user,
					passwd = self.args.src_password
				)
				if self.args.src_port is not None:
					dns["port"] = self.args.src_port
				self.conn = MySQLdb.connect(**dns)
				self.c = self.conn.cursor()
				print "Successfully connected to mysql database!"
			except Exception, e:
				try:
					print "Failed to connect to mysql database db. Reason: ", e 
					print "Trying oracle instead ..."
					import cx_Oracle
					connection_string = self.args.src_user + "/" + self.args.src_password + "@" + self.args.src_host + ":" + str(self.args.src_port) + "/" + self.args.src_database
					self.conn = cx_Oracle.Connection(connection_string)
					self.c = cx_Oracle.Cursor(self.conn)
					self.TYPE = "oracle"
				except Exception, e:
					print >> sys.stderr, "Could not connect to source database:", e
					sys.exit(1)


	
class VermontDB(BaseImporter):
	def __init__(self, args):
		BaseImporter.__init__(self, args)
		self.get_db_connection()
		self.flows = None
		self.get_tables();
		self.prevFlow = None
		print self.tables;
		if self.args.table_name:
			print "Limiting table space to ", self.args.table_name
			if self.args.table_name in self.tables:
				self.tables = [ self.args.table_name ]
			else:
				print "Table " + self.args.table_name + " is not in database!"
				self.tables = []
				

	def get_next_flow(self):
		if self.prevFlow:
			ret = self.prevFlow
			self.prevFlow = None
			return ret
	
		flow = self.c.fetchone()

		if flow == None:
			if len(self.tables) == 0:
				return None
			table =  self.tables.pop()

			print "Importing table ", table, "..."
			self.c.execute("SELECT * FROM " + table)
			return self.get_next_flow()


		obj = dict()
		revObj = dict()
		haveReverse = False
		for j, colDesc in enumerate(self.c.description):
			# oracle returns column names in upper cases, which we do not expect
			# translate the name to what we expect and what we get from all the
			# normal databases
			if self.TYPE == "oracle":
				if colDesc[0].startswith('REV'):
					fieldName = colDesc[0][3:len(col[0])]
					if fieldName in common.ORACLE_COLUMNMAP:
						col = 'rev' + common.ORACLE_COLUMNMAP[fieldName]
					else:
						# will probably be ignored
						col = colDesc[0]
				else:
					if colDesc[0] in common.ORACLE_COLUMNMAP:
						col = common.ORACLE_COLUMNMAP[colDesc[0]]
					else:
						col = colDesc[0]
			else:
				col = colDesc[0]

			if col not in common.IGNORE_COLUMNS:
				# vermont tables may contain reverse flow information
				# if they have been produced using the biflowaggregation feature. 
				# create a revObj which matches the reverse flow
				if col == common.COL_SRC_IP:
					revObj[common.COL_DST_IP] = flow[j]
				if col == common.COL_DST_IP:
					revObj[common.COL_SRC_IP] = flow[j]
				if col == common.COL_SRC_PORT:
					revObj[common.COL_DST_PORT] = flow[j]
				if col == common.COL_DST_PORT:
					revObj[common.COL_SRC_PORT] = flow[j]
				if col == common.COL_PROTO:
					revObj[common.COL_PROTO] = flow[j]

				if col.startswith('rev'):
					haveReverse = True
					fieldName = col[3:len(col)]
					# vermont flows can contain MilliSecond fields instead of second fields
					# flow-inspector requires seconds. hence, we produce the second fields
					# from the milliseconds
					if fieldName == "flowStartMilliSeconds":
						revObj[common.COL_FIRST_SWITCHED] = flow[j] / 1000
					elif fieldName == "flowEndMilliSeconds":
						revObj[common.COL_LAST_SWITCHED] = flow[j] / 1000
					revObj[fieldName] = flow[j]
				else:
	
					# vermont flows can contain MilliSecond fields instead of second fields
					# flow-inspector requires seconds. hence, we produce the second fields
						# from the milliseconds
					if col == "flowStartMilliSeconds":
							obj[common.COL_FIRST_SWITCHED] = flow[j] / 1000
					elif col == "flowEndMilliSeconds":
						obj[common.COL_LAST_SWITCHED] = flow[j] / 1000
					obj[col] = flow[j]

		# if we have at least one reverse flow field, we need to create the reverse field
		# we will store the reversefield and return it on the next call to get_next_flow
		if haveReverse:
			# check if the reverse flow start time is > 0. if it is zero, this means that we have a 
			# one way flow (e.g. coming from a scan) that should not be imported into the database
			if common.COL_FIRST_SWITCHED in revObj and revObj[common.COL_FIRST_SWITCHED] > 0:
				self.prevFlow = revObj

		return obj

	
	
	def compareTables(a, b):
		compsA = a.split('_')
		compsB = b.split('_')
		return -cmp(int(compsA[1]), int(compsB[1])) or cmp(int(compsA[2]), int(compsB[2])) or cmp(int(compsA[3]), int(compsB[3]))


	def get_tables(self):
		# get all flow tables
		if self.TYPE == "oracle":
			self.c.execute("""SELECT * FROM user_objects WHERE object_type = 'TABLE' AND object_name LIKE 'F!_%' ESCAPE '!'""")
		else:
			query_string = """SELECT table_name from information_schema.tables 
				WHERE table_schema="%s" AND table_type='BASE TABLE' AND table_name LIKE 'f\\_%%' ORDER BY table_name ASC""" % (self.args.src_database)
			self.c.execute(query_string)
		print "Getting all table names ..."
		self.tables = self.c.fetchall()
	
		# get the table names in list format
		self.tables = map(lambda x: x[0], list(self.tables))
	
		if self.TYPE == "oracle":
			self.tables = sorted(self.tables, compareTables)
		else:
			self.tables.sort()


class LegacyVermontDB(BaseImporter):
	def __init__(self, args):
		BaseImporter.__init__(self, args)
		self.get_db_connection()
		self.flows = None
		self.get_tables;

	def get_next_flow(self):
		if self.args.table_name:
			if self.args.table_name in self.tables:
				self.tables = [ self.args.table_name ]
			else:
				# the requested table is not known!
				return None

		if self.flows == None:
			if len(self.tables) == 0:
				return None
			table = self.tables.pop()
			print "Importing table ", table, "..."
		
			self.c.execute("SELECT * FROM " + table + " ORDER BY FIRSTSWITCHED ASC")
			self.flows = self.c.fetchall()

		if len(self.flows) >= 1:
			flow =  self.flows.pop()
		else:
			flow = None
		if len(self.flows) == 0:
			self.flows = None

		if flow != None:
			# remove legacy names and return object with IPFIX names
			obj = dict()
			for j, col in enumerate(self.c.description):
				if col[0] not in common.IGNORE_COLUMNS:
					if col[0].upper() in common.LEGACY_COLUMNMAP:
						obj[common.LEGACY_COLUMNMAP[col[0].upper()]] = row[j]
					else:
						obj[col[0]] = row[j]
			return obj

		return flow;
	
	
	def compareTables(a, b):
		compsA = a.split('_')
		compsB = b.split('_')
		return -cmp(int(compsA[1]), int(compsB[1])) or cmp(int(compsA[2]), int(compsB[2])) or cmp(int(compsA[3]), int(compsB[3]))


	def get_tables(self):
		# get all flow tables
		if self.TYPE == "oracle":
			c.execute("""SELECT * FROM user_objects WHERE object_type = 'TABLE' AND object_name LIKE 'H!_%' ESCAPE '!'""")
		else:
			c.execute("""SELECT table_name from information_schema.tables 
				WHERE table_schema=%s AND table_type='BASE TABLE' AND table_name LIKE 'h\\_%%' ORDER BY table_name ASC""", (args.src_database))
		print "Getting all table names ..."
		self.tables = c.fetchall()
	
		# get the table names in list format
		self.tables = map(lambda x: x[0], list(self.tables))
	
		if self.TYPE == "oracle":
			self.tables = sorted(self.tables, compareTables)
		else:
			self.tables.sort()



class BroImporter(BaseImporter):
	def __init__(self, args):
		BaseImporter.__init__(self, args)
		if not args.conn_file:
			print "missing argument --conn-file!"
			sys.exit(-1)

		try:
			self.input_file = open(args.conn_file, "r")
		except Exception as e:
			print >> sys.stderr, "Could not open connection log file ", args.conn_file, ": ", e
			sys.exit(1)

		self.prevFlow = None


	def get_next_flow(self):
		if self.prevFlow:
			ret = self.prevFlow
			self.prevFlow = None
			return ret

		line = self.input_file.readline()
		if not line:
			return None
		# we only support the default format at the moment. 
		# TODO: parse the header, get the appropriate names and 
		# field separators ...
		if line.startswith('#'):
			return self.get_next_flow()
	
		fields = line.split()	
	
		srcFlow = {}
		srcFlow[common.COL_FIRST_SWITCHED] = float(fields[0])
		srcFlow[common.COL_SRC_IP] = ip2int(fields[2])
		srcFlow[common.COL_SRC_PORT] = int(fields[3])
		srcFlow[common.COL_DST_IP] = ip2int(fields[4])
		srcFlow[common.COL_DST_PORT] = int(fields[5])
		srcFlow[common.COL_PROTO] = common.getValueFromProto(fields[6])
		if fields[8] == '-':
			srcFlow[common.COL_LAST_SWITCHED] = float(fields[0])
		else:
			srcFlow[common.COL_LAST_SWITCHED] = float(fields[0]) + float(fields[8])
		srcFlow[common.COL_PKTS] = int(fields[15])
		srcFlow[common.COL_BYTES] = int(fields[16])
	
		dstFlow = {}
		dstFlow[common.COL_FIRST_SWITCHED] = float(fields[0])
		dstFlow[common.COL_SRC_IP] = ip2int(fields[4])
		dstFlow[common.COL_SRC_PORT] = int(fields[5])
		dstFlow[common.COL_DST_IP] = ip2int(fields[2])
		dstFlow[common.COL_DST_PORT] = int(fields[3])
		dstFlow[common.COL_PROTO] = common.getValueFromProto(fields[6])
		if fields[8] == '-':
			dstFlow[common.COL_LAST_SWITCHED] = float(fields[0])
		else:
			dstFlow[common.COL_LAST_SWITCHED] = float(fields[0]) + float(fields[8])
		dstFlow[common.COL_PKTS] = int(fields[17])
		dstFlow[common.COL_BYTES] = int(fields[18])
	
		# return srcFlow now, return dstflow at the next call to get_next_flow()
		self.prevFlow = dstFlow
		return srcFlow

class ArgusDB(BaseImporter):
	def __init__(self, args):
		BaseImporter.__init__(self, args)
		self.get_db_connection()
		self.flows = None
		self.get_tables();
		self.prevFlow = None
		if self.args.table_name:
			if self.args.table_name in self.tables:
				self.tables = [ self.args.table_name ]
			else:
				print "Table " + self.args.table_name + " is not in database!"
				self.tables = []
		self.columnmap = {
			"proto": common.COL_PROTO,
			"saddr": common.COL_SRC_IP,
			"daddr": common.COL_DST_IP,
			"sport": common.COL_SRC_PORT,
			"dport": common.COL_DST_PORT
		}
		self.srcDirectionMap = {
			"spkts" : common.COL_PKTS,
			"sbytes": common.COL_BYTES
		}
		self.dstDirectionMap = {
			"dpkts" : common.COL_PKTS,
			"dbytes" : common.COL_BYTES
		}
				

	def get_next_flow(self):
		if self.prevFlow:
			ret = self.prevFlow
			self.prevFlow = None
			return ret
	
		flow = self.c.fetchone()
		if flow == None:
			if len(self.tables) == 0:
				return None
			table =  self.tables.pop()

			print "Importing table ", table, "..."
			self.c.execute("SELECT * FROM " + table + " ORDER BY stime ASC")
			return self.get_next_flow()

		obj = dict()
		revObj = dict()
		firstSwitched = None
		for j, col in enumerate(self.c.description):
			print col[0]
			if col[0] in self.columnmap:
				fieldName = self.columnmap[col[0]]
				print "fieldName: ", fieldName
				# argus tables contain reverse flow information
				# create a revObj which matches the reverse flow
				if fieldName == common.COL_SRC_IP:
					ip = ip2int(flow[j])
					obj[common.COL_SRC_IP] = ip
					revObj[common.COL_DST_IP] = ip
				if fieldName == common.COL_DST_IP:
					ip = ip2int(flow[j])
					obj[common.COL_DST_IP] = ip
					revObj[common.COL_SRC_IP] = ip
				if fieldName == common.COL_SRC_PORT:
					obj[common.COL_SRC_PORT] = flow[j]
					revObj[common.COL_DST_PORT] = flow[j]
				if fieldName == common.COL_DST_PORT:
					obj[common.COL_DST_PORT] = flow[j]
					revObj[common.COL_SRC_PORT] = flow[j]
				if fieldName == common.COL_PROTO:
					obj[common.COL_PROTO] = common.getValueFromProto(flow[j])
					revObj[common.COL_PROTO] = common.getValueFromProto(flow[j])
			elif col[0] in self.srcDirectionMap:
				fieldName = self.srcDirectionMap[col[0]]
				obj[fieldName] = flow[j]
			elif col[0] in self.dstDirectionMap:
				fieldName = self.dstDirectionMap[col[0]]
				revObj[fieldName] = flow[j]
			elif col[0] == "stime":
				firstSwitched = float(flow[j])
				obj[common.COL_FIRST_SWITCHED] = firstSwitched
				revObj[common.COL_FIRST_SWITCHED] = firstSwitched
			elif col[0] == "dur":
				obj[common.COL_LAST_SWITCHED] = firstSwitched + float(flow[j])
				revObj[common.COL_LAST_SWITCHED] = firstSwitched + float(flow[j])
				

		# check if the reverse flow start time is > 0. if it is zero, this means that we have a 
		# one way flow (e.g. coming from a scan) that should not be imported into the database
		if common.COL_FIRST_SWITCHED in revObj and revObj[common.COL_FIRST_SWITCHED] > 0:
			self.prevFlow = revObj

		print obj
		return obj

	def get_tables(self):
		query_string = """SELECT table_name from information_schema.tables 
			WHERE table_schema="%s" AND table_type='BASE TABLE' ORDER BY table_name ASC""" % (self.args.src_database)
		self.c.execute(query_string)
		print "Getting all table names ..."
		self.tables = self.c.fetchall()

		# get the table names in list format
		self.tables = map(lambda x: x[0], list(self.tables))

	
def get_importer_module(importer_type, args):
	if importer_type == "vermont-db":
		return VermontDB(args);
	elif importer_type == "legacy-vermont-db":
		return LegacyVermontDB(args)
	elif importer_type == "bro-importer":
		return BroImporter(args)
	elif importer_type == "argus-importer":
		return ArgusDB(args)
	else:
		print "Unsupported importer module " + importer_type
		sys.exit(-1)
