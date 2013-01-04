
class AnalysisBase:
	def __init__(self, flowbackend, databackend):
		self.flowBackend = flowbackend
		self.dataBackend = databackend
	
	def analyze(self, startBucket, endBucket):
		pass
