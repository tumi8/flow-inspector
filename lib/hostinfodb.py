import sys
import config


hostDBFields = {
	"hostIP": "IP"
}

class HostInfoDB:
	def __init__(self):
		try:
			import cx_Oracle
			connection_string = config.host_info_user + "/" + config.host_info_password + "@" + config.host_info_host + ":" + str(config.host_info_port) + "/" +config.host_info_name
			self.conn = cx_Oracle.Connection(connection_string)
			self.cursor = cx_Oracle.Cursor(self.conn)
		except Exception, e:
			print >> sys.stderr, "Could not connect to HostInfoDB: ", e
			sys.exit(-1)

	def run_query(self, tableName, query):
		query = query % (tableName)
		print query
		self.cursor.execute(query)
		return self.cursor.fetchall()

