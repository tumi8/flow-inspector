#!/usr/bin/env python

import sys
import os
import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))

import config
import backend
import mail

def report_backend_usage(dst_db):
	report_string = ""
	for table in  dst_db.getCollectionList():
		coll = dst_db.getCollection(table)
		firstTime = None
		lastTime = None
		try:
			firstTime = coll.min("timestamp")
			lastTime  = coll.max("timestamp")
		except Exception as e:
			# no timestamp in the table
			pass
		report_string +=  "Table:  " +  table.ljust(30) +  ' Number of entries:  ' + str(coll.count()).ljust(10)
		if firstTime != None:
			report_string += "First: " + str(datetime.datetime.fromtimestamp(firstTime)).ljust(10) + " Last: " + str(datetime.datetime.fromtimestamp(lastTime)).ljust(10) + '\n'
		else:
			report_string += "\n"
	return report_string

if __name__ == "__main__":
	# check the flow backend
	dst_db = backend.flowbackend.getBackendObject(
		config.db_backend, config.db_host, config.db_port,
		config.db_user, config.db_password, config.db_name)

	report_string = "Usage report for flowbackend: \n\n"
	report_string += report_backend_usage(dst_db)

	# check if databackend differs from flowbackend
	# only report  statistics if they are two different backends.
	# If the backends for flows and data are the same backends, 
	# then all data tables will be reported by the previous 
	# step
	if config.db_backend != config.data_backend or config.db_host != config.data_backend_host or config.db_port != config.data_backend_port or  config.db_user != config.data_backend_user or config.db_password != config.data_backend_password or config.db_name != config.data_backend_password:
	   	dst_db = backend.databackend.getBackendObject(
			config.data_backend, config.data_backend_host, config.data_backend_port,
			config.data_backend_user, config.data_backend_password, config.data_backend_database)
		report_string += "\n\nUsage report for databackend:\n\n"
		report_string += report_backend_usage(dst_db)		

	mail.send_mail("Status Report on Database Usage for " + str(datetime.datetime.now()), report_string)
