from analysis_base import AnalysisBase

import config
import common

class NetworkScanDetector(AnalysisBase):
	def __init__(self, backend):
		AnalysisBase.__init__(self, backend)

	def analyze(self, startBucket, endBucket):
		tableName = common.DB_FLOW_PREFIX + str(self.backend.getBucketSize(startBucket, endBucket, 1000))
		print self.backend.run_query(tableName, "SELECT proto, srcIP, dstIP, dstPort, COUNT(DISTINCT dstIP) AS di from %s WHERE pkts <= 3 AND (proto != 1 OR dstPort = 2048) GROUP BY proto, srcIP, dstPort HAVING di >= 50 ORDER BY di DESC");
