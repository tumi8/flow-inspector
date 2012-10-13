#!/usr/bin/env python 

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

from network_scan_detector import NetworkScanDetector
import backend
import config
import common

if __name__ == "__main__":
	db = backend.getBackendObject(config.db_backend, config.db_host, config.db_port, config.db_user, config.db_password, config.db_name)

	if len(sys.argv) == 2:
		startBucket = int(sys.argv[1])
		endBucket = int(sys.argv[2])
		print "Using start and endtime from command line arguments. startbucket: ", startBucket, " endBucket: ", endBucket
	else:
		startBucket = db.getMinBucket()
		endBucket = db.getMaxBucket()
		print "Analysing data in buckets: [", startBucket, ", ", endBucket, "]"

	networkScanDetector = NetworkScanDetector(db)

	# use smallest bucket size 
	slidingBucketSize = config.flow_bucket_sizes[0]
	for bucket in range(startBucket, endBucket, slidingBucketSize):
		print bucket
		networkScanDetector.analyze(startBucket, startBucket)
