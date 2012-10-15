from analysis_base import AnalysisBase

import sys
import datetime

import config
import common
import time

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
			#sys.exit(-1)

	def run_query(self, tableName, query):
		return []
		query = query % (tableName)
		self.cursor.execute(query)
		return self.fetchall()

class HostInformationChecker(AnalysisBase):
	def __init__(self, flowbackend, databackend):
		AnalysisBase.__init__(self, flowbackend, databackend)
		self.hostInfoDB = HostInfoDB()

		self.hiCollectionName = "HOST_INFORMATION_CHECKER"

		self.analysisResultDict = {
			"IP" : ("INTEGER(10) UNSIGNED", "PRIMARY"),
			"LASTSEEN" : ("INTEGER(10) UNSIGNED", None),
			"LASTINFOCHECK" : ("INTEGER(10) UNSIGNED", None)
		}
		self.dataBackend.prepareCollection(self.hiCollectionName, self.analysisResultDict)

	def analyze(self, startBucket, endBucket):
		tableName = common.DB_FLOW_PREFIX + str(self.flowBackend.getBucketSize(startBucket, endBucket, 1000))
		srcIPs = self.flowBackend.run_query(tableName, "SELECT srcIP, max(bucket) as bucket from %s GROUP BY srcIP");
		for ip in srcIPs:
			hostInfo = self.hostInfoDB.run_query(tableName, "SELECT * FROM %s WHERE IP = " + int2ip(ip['srcIP']))
			currentTime = int(time.time())
			if len(hostInfo) == 1:
				# that's what we expect. An entry was found!
				pass
			elif len(hostInfo) == 0:
				ipdict = {
					"IP" : ip['srcIP'],
					"LASTSEEN": ip['bucket'],
					"LASTINFOCHECK" : currentTime
				}
				self.dataBackend.insert(self.hiCollectionName, ipdict)
			else:
				# got more than one entry for this ip. This should not happen and should be 
				# reported separately
				print "Found multiple entries for host:", int2ip(ip['srcIP'])

