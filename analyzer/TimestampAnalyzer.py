from EWMAAnalyzer import EWMAAnalyzer

class TimestampAnalyzer(EWMAAnalyzer):

	@staticmethod
	def getInstances(data):
		return ((str(router) + "-" + str(interface), (router, interface, "timestamp")) for router in data for interface in data[router])
