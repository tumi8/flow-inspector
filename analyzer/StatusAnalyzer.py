import analyzer

class StatusAnalyzer(analyzer.Analyzer):
	
	def __init__(self, router, interface):
		self.router = router
		self.interface = interface

	def passDataSet(self, data):
		router = self.router
		interface = self.interface
		record = data[router][interface]
		if (record["ifOperStatus"] != record["ifAdminStatus"]):
			print "---> Mismatch"
			print time.strftime("%d/%m/%y %H:%M:%S", time.localtime(record["timestamp"])), record["router"], record["ifIndex"], record["ifLastChange"], record["ifAdminStatus"], record["ifOperStatus"]

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface)) for router in data for interface in data[router])
