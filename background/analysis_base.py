
class AnalysisBase:
	def __init__(self, flowbackend, databackend):
		self.dataBackend = databackend
		self.flowBackend = flowbackend
	
	def analyze(self, startBucket, endBucket):
		pass
