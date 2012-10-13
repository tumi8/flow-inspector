from analysis_base import AnalysisBase

import sys

import config
import common

hostDBFields = {
	"hostIP": "IP"
}

def int2ip(i):
	# konvertiert einen Integer in eine IP im dotted-quad Format
	return str(i//2**24)+"."+str((i//2**16)%256)+"."+str((i//2**8)%256)+"."+str(i%256)

class HostInfoDB:
	def __init__(self):
		try:
			import cx_Oracle
			connection_string = config.db_user + "/" + config.db_password + "@" + config.db_host + ":" + str(config.db_port) + "/" +config.db_database
			self.conn = cx_Oracle.Connection(connection_string)
			self.cursor = cx_Oracle.Cursor(conn)
		except Exception, e:
			print >> sys.stderr, "Could not connect to HostInfoDB: ", e
			sys.exit(-1)

	def run_query(self, tableName, query):
		query = query % (tableName)
		self.cursor.execute(query)
		return self.fetchall()

class HostInformationChecker(AnalysisBase):
	def __init__(self, backend):
		AnalysisBase.__init__(self, backend)
		self.hostInfoDB = HostInfoDB()

	def analyze(self, startBucket, endBucket):
		tableName = common.DB_FLOW_PREFIX + str(self.backend.getBucketSize(startBucket, endBucket, 1000))
		srcIPs = self.backend.run_query(tableName, "SELECT DISTINCT(srcIP) from %s");
		for ip in srcIPs:
			hostInfo = self.hostInfoDB.run_query(tableName, "SELECT * FROM %s WHERE IP = " + int2ip(ip['srcIP']))
			if len(hostInfo) == 1:
				# that's what we expect. An entry was found!
				pass
			elif len(hostInfo) == 0:
				print "Could not find info for host ", int2ip(ip['srcIP'])
			else:
				# got more than one entry for this ip. This should not happen and should be 
				# reported separately
				print "Found multiple entries for host:", int2ip(ip['srcIP'])

