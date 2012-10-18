from analysis_base import AnalysisBase

import sys
import datetime

import config
import common
import time
import hostinfodb

def int2ip(i):
	# konvertiert einen Integer in eine IP im dotted-quad Format
	return str(i//2**24)+"."+str((i//2**16)%256)+"."+str((i//2**8)%256)+"."+str(i%256)


class HostInformationChecker(AnalysisBase):
	def __init__(self, flowbackend, databackend):
		AnalysisBase.__init__(self, flowbackend, databackend)
		self.hostInfoDB = hostinfodb.HostInfoDB()

		self.hiCollectionName = common.HOST_INFORMATION_COLLECTION

		self.analysisResultDict = {
			"IP" : ("NUMBER(10)", "PRIMARY"),
			"LASTSEEN" : ("NUMBER(10)", None),
			"LASTINFOCHECK" : ("NUMBER(10)", None)
		}
		self.dataBackend.prepareCollection(self.hiCollectionName, self.analysisResultDict)

	def analyze(self, startBucket, endBucket):
		tableName = common.DB_FLOW_PREFIX + str(self.flowBackend.getBucketSize(startBucket, endBucket, 1000))
		srcIPs = self.flowBackend.run_query(tableName, "SELECT " + common.COL_SRC_IP + ", max(" + common.COL_BUCKET + ") as " + common.COL_BUCKET + " from %s GROUP BY " + common.COL_SRC_IP);
		for ip in srcIPs:
			hostInfo = self.hostInfoDB.run_query(config.host_information_table, "select case when exists(select 1 from %s where ip='" + int2ip(ip[0]) + "') then 'Y' else 'N' end as rec_exists from dual")
			currentTime = int(time.time())
			if hostInfo[0][0] == 'Y':
				# that's what we expect. An entry was found!
				pass
			else:
				ipdict = {
					"IP" : (ip[0], "PRIMARY"),
					"LASTSEEN": (ip[1], "UPDATE"),
					"LASTINFOCHECK" : (currentTime, "UPDATE")
				}
				self.dataBackend.insert(self.hiCollectionName, ipdict)

