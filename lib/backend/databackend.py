"""
Flow Inspector maintains a number of information that are different from flow data.
This information includes the system configuration, or analysis results of background
data. 
Because a user might decide to store his flows in some kind of specialized flow database,
another backend is necessary if the flow backend does not support data other than flows
(this can for example happen if the nfdump or SiLK tools are used to manage the flow
information).
Supported backends:
	- mysql
	- oracle

Author: Lothar Braun 
"""

import sys
import flowbackend

def getBackendObject(backend, host, port, user, password, databaseName):
	if backend == "mongo":
		from mongobackend import MongoBackend
		return MongoBackend(host, port, user, password, databaseName)
	elif backend == "mysql":
		from mysqlbackend import MysqlBackend
		return MysqlBackend(host, port, user, password, databaseName)
	elif backend == "oracle":
		from oraclebackend import OracleBackend
		return OracleBackend(host, port, user, password, databaseName)
	else:
		raise Exception("Backend " + backend + " is not a supported backend")	if backend == "mysql":
