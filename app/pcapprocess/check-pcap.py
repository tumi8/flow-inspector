#!/usr/bin/env python

import sys, os, struct 
import subprocess
from optparse import OptionParser
import pymongo


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor', 'dpkt-1.7'))
import dpkt


#|#################### global 
connections = {}
IPADDR_BINARY = struct.Struct("!BBBB")

##################### classes

class Unbuffered:
	def __init__(self, stream):
		self.stream = stream
	def write(self, data):
		self.stream.write(data)
		self.stream.flush()
	def __getattr__(self, attr):
		return getattr(self.stream, attr)

class SequenceNumberAnalyzer:
	def __init__(self, cli, serv, connString):
		self.cli = cli
		self.serv = serv
		self.connString = connString

		# next expected client sequence number
		self.cliSeq = 0
		# next expected server sequence number
		self.servSeq = 0

		self.unexpectCliServ = 0
		self.unexpectServCli = 0

		self.SYNSeen = False
		self.lastSYNSeen = 0
		self.lastSYNACKSeen = 0
		self.SYNACKSeen = False

		self.SYNFromServ = False

		self.cliRSTSeen = False
		self.servRSTSeen = False
		
		self.cliFINSeen = False
		self.servFINSeen = False

	def isRunning(self):
		# try to estimate if a TCP connection can still expect more packets
		if self.servRSTSeen or self.cliRSTSeen:
			print self.connString + " self.servRSTSeen: " + str(self.servRSTSeen), " self.cliRSTSeen: " + str(self.cliRSTSeen)
			return False
		if self.servFINSeen and self.cliFINSeen:
			print self.connString + " self.servFINSeen: " + str(self.servFINSeen), " self.cliFINSeen: " + str(self.cliFINSeen)
			return False

		# be conservative (we need to think about this one here ... TODO, FIXME)
		return True

	def nextPacket(self, ipPacket, timestamp):
		if type(ipPacket.data) != dpkt.tcp.TCP:
			print "SequenceNumberAnalyzer got non TCP packet!"
			return
		tcpSegment = ipPacket.data
		src = ipPacket.src
		dst = ipPacket.dst

		if src == self.cli:
			# received packet from client to server
			if tcpSegment.flags & dpkt.tcp.TH_SYN:
				if self.SYNSeen == False:
					self.SYNSeen = True
					self.lastSYNSeen = timestamp
					self.cliSeq = tcpSegment.seq + 1
					#print "Seen SYN for ", self.connString
				else:
					print "Duplicate SYN in " + self.connString + ". Old ts: " + str(self.lastSYNSeen) + ", new ts: " + str(timestamp)
					if self.cliSeq != tcpSegment.seq + 1:
						print "\tduplicate SYN has different sequence number. Old: %d | New: %d" % (self.cliSeq - 1, tcpSegment.seq)
				return

			# check for RST and FIN flags
			if tcpSegment.flags & dpkt.tcp.TH_RST:
				if self.cliFINSeen:
					#print "Reset from client after FIN in " + self.connString
					pass
				else:
					print "Reset from client without FIN in " + self.connString + " at " + str(timestamp)
				self.cliRSTSeen = True

			if tcpSegment.flags & dpkt.tcp.TH_FIN:
				if self.cliFINSeen:
					print "Duplicate FIN from client in " + self.connString + " at " + str(timestamp)
				else:
					#print "FIN from client seen in " + self.connString
					pass
				self.cliFINSeen = True

			# normal packet. 
			#print "Stats: ", ipPacket.len, ipPacket.hl * 4, tcpSegment.off * 4, "(", self.connString, ")"
			segPayloadLen = ipPacket.len - (ipPacket.hl * 4 + tcpSegment.off * 4)
			if segPayloadLen == 0:
				# fin packets add one to the sequence number
				if tcpSegment.flags & dpkt.tcp.TH_FIN:
					segPayloadLen = 1

			# check if we received a SYN
			if self.cliSeq == 0:
				#print "In %s: Expected seq: %d. Got seq: %d, Diff: %d, Payload len: %d" % (self.connString, self.cliSeq, tcpSegment.seq, self.cliSeq - tcpSegment.seq, segPayloadLen)
				self.cliSeq = tcpSegment.seq + segPayloadLen
				return

			if self.cliSeq != tcpSegment.seq:
				#print "In %s: Expected seq: %d. Got seq: %d, Diff: %d, Payload len: %d" % (self.connString, self.cliSeq, tcpSegment.seq, self.cliSeq - tcpSegment.seq, segPayloadLen)
				self.unexpectCliServ += 1
			else:
				#print "In %s: Good     seq: %d, Got seq: %d, Diff: %d, Payload len: %d" % (self.connString, self.cliSeq, tcpSegment.seq, self.cliSeq - tcpSegment.seq, segPayloadLen)
				pass
			self.cliSeq = tcpSegment.seq + segPayloadLen
			#if not tcpSegment.seq == self.cliSeq
				
		elif src == self.serv:
			# received packet from server to client	
			if tcpSegment.flags & dpkt.tcp.TH_SYN:
				if not tcpSegment.flags & dpkt.tcp.TH_ACK:
					print "Weird: Got SYN from server in " + self.connString + " at " + str(ts)
					self.SYNFromServ = True
				else:
					# syn ack
					if self.SYNACKSeen:
						print "Duplicate SYN/ACK in " + self.connString + ". Old ts: " + str(self.lastSYNACKSeen) + ", new ts: " + str(timestamp)
						self.lastSYNACKSeen = timestamp
 
						if self.servSeq != tcpSegment.seq + 1:
							print "\tduplicate SYN/ACK has different sequence number. Old: %d | New: %d" % (self.servSeq - 1, tcpSegment.seq)
					else:
						self.SYNACKSeen = True
							
					self.servSeq = tcpSegment.seq + 1
				return

			# check for RST and FIN flags
			if tcpSegment.flags & dpkt.tcp.TH_RST:
				if self.servFINSeen:
					#print "Reset from server after FIN in " + self.connString
					pass
				else:
					print "Reset from server without FIN in " + self.connString + " at " + str(timestamp)
					pass
				self.servRSTSeen = True

			if tcpSegment.flags & dpkt.tcp.TH_FIN:
				if self.servFINSeen:
					print "Duplicate FIN from server in " + self.connString + " at " + str(timestamp)
				else:
					#print "FIN from server seen in " + self.connString
					pass
				self.servFINSeen = True

			# normal packet
			segPayloadLen = ipPacket.len - (ipPacket.hl * 4 + tcpSegment.off * 4)
			if self.servSeq == 0:
				self.servSeq = tcpSegment.seq + segPayloadLen
				return

			if self.servSeq != tcpSegment.seq:
				self.unexpectServCli += 1

			self.servSeq = tcpSegment.seq + segPayloadLen

class ConnectionRecord:
	def __init__(self, connString, ts, src, dst, sPort, dPort, proto, pString):
		self.firstTs = ts;
		self.lastTs = ts;
		self.connString = connString

		self.src = src
		self.dst = dst
		self.sPort = sPort
		self.dPort = dPort
		self.proto = proto
		self.pString = pString

		self.numPkts = 0
		self.numBytes = 0

		self.maxDiff = 0;
		self.diffs = [];
		self.avgDiff = 0;

		self.writer = None
		
		if proto == dpkt.tcp.TCP:
			self.seqAnalyzer = SequenceNumberAnalyzer(src, dst, self.connString)
		else:
			self.seqAnalyzer = None

	def packet_seen(self, ts, pkt, eth):
		# calculate the maximum diff between two frames
		if (ts - self.lastTs) > self.maxDiff:
			self.maxDiff = ts - self.lastTs

		# update the average diff between two frames
		currDiff = ts - self.lastTs
		self.diffs.append(currDiff)
		if self.numPkts == 1:
			self.avgDiff = currDiff
		else:
			# incremental update 
			self.avgDiff = self.avgDiff + (currDiff - self.avgDiff) / (self.numPkts + 1)

		self.lastTs = ts
		self.numPkts += 1
		self.numBytes += eth.data.len

		if self.seqAnalyzer:
			self.seqAnalyzer.nextPacket(eth.data, ts)

	def calcAvgThroughput(self):
		if (self.lastTs - self.firstTs) == 0:
			return 0
		return self.numBytes / (self.lastTs - self.firstTs) * 8;

	def calcMedianDiff(self):
		self.diffs.sort()
		samples = len(self.diffs)
		if samples == 1:
			return self.diffs[0]
		elif samples % 2 == 0:
			return self.diffs[samples/2]
		else:
			return (self.diffs[samples/2] + self.diffs[samples/2 + 1]) / 2


	def createWriter(self, path, snaplen):
		self.writer = dpkt.pcap.Writer(open(path, 'w'), snaplen=snaplen)

	def write(self, ts, buf):
		# check fi we have a writer
		if self.writer == None:
			return 
		
		# sanity check performed. write hte packet
		self.writer.writepkt(buf, ts)

	def dump_stat(self, collection):
		doc = { "firstSwitched": self.firstTs, "lastSwitched": self.lastTs, "srcIP": binary_to_str(self.src), "dstIP": binary_to_str(self.dst), "srcPort": self.sPort, "dstPort": self.dPort, "proto": self.pString, "pkts": self.numPkts, "bytes": self.numBytes, "maxDiff": self.maxDiff, "avgDiff": self.avgDiff, "medianDiff": self.calcMedianDiff(), "avgThroughput": self.calcAvgThroughput() }
		collection.save(doc)

##################### functions


def binary_to_str(binaddr):
	return "%d.%d.%d.%d" % IPADDR_BINARY.unpack(binaddr)

def create_conn_id(srcIP, dstIP, srcPort, dstPort, proto):
	# build connection identifier
	# make biflow identifier
	# TODO we also map two connections
	#		ip1, p1, ip2, p2
	#		ip1, p2, ip2, p1
	#	onto the same identifier. this is problematic

	# 	fix this 
	if srcIP > dstIP:
		(srcIP, dstIP) = (dstIP, srcIP)
	if srcPort > dstPort:
		(srcPort, dstPort) = (dstPort, srcPort)
	return srcIP + dstIP + str(srcPort) + str(dstPort) + str(proto)


def dump_conn_to_result_db(conn, options, collections):
	(withGaps, flowCollection, lowTrhoughput) = collections
	if conn.maxDiff > options.maxGap:
		conn.dump_stat(withGaps)
	conn.dump_stat(flowCollection)
	if conn.numBytes >= options.minLenThroughput:
		if conn.calcAvgThroughput() < options.minThroughput:
			conn.dump_stat(lowThroughput)

############################# main 
		


def main(options, collections):

	# open pcap file
	if options.inputFile != "-":
		f = open(options.inputFile)
	else:
		f = sys.stdin
	pcapFile = dpkt.pcap.Reader(f)
	if options.pcapFilter != None:
		print "Setting pcap filter: ", options.pcapFilter
		pcapFile.setfilter(options.pcapFilter)

	# create output directory if it does not exist
	if not os.access(options.outputDir, os.R_OK | os.W_OK):
		os.makedirs(options.outputDir)

	(flowCollection, lowThroughput, withGaps, pcapStats) = collections

	progressOutput = Unbuffered(open(os.path.join(options.outputDir, "analysis-output.txt"), 'w+'))

	progressOutput.write("starting to read packets ...\n")
	unsupported = 0
	seen = 0

	lastSec = 0
	allPkts = 0
	allBytes = 0
	tcpPkts = 0
	tcpBytes = 0
	udpBytes = 0
	udpPkts = 0
	otherPkts = 0
	otherBytes = 0

	try:
		for ts, buf in pcapFile:
			if lastSec == 0:
				lastSec = ts
			if int(ts) > lastSec:
				doc = {
					"second": lastSec,
					"allPkts": allPkts,
					"allBytes": allBytes,
					"tcpPkts": tcpPkts,
					"tcpBytes": tcpBytes,
					"udpPkts": udpPkts,
					"udpBytes": udpBytes,
					"otherPkts": otherPkts,
					"otherBytes": otherBytes
				}
				pcapStats.save(doc)
				lastSec = ts
				allPkts = allBytes = tcpPkts = tcpBytes = udpPkts = udpBytes = otherPkts = otherBytes = 0
	
			seen += 1
			eth = dpkt.ethernet.Ethernet(buf)
			srcIP = 0
			dstIP = 0
			srcPort = 0
			dstPort = 0
			proto = ""

			allPkts += 1
			if type(eth.data) == dpkt.ip.IP:
				allBytes += eth.data.len
			else:
				allBytes += 28 # we only consider standard arp over ethernet ... TODO

			if type(eth.data) == dpkt.ip.IP:
				ip_packet = eth.data
				srcIP = ip_packet.src
				dstIP = ip_packet.dst
				if type(ip_packet.data) == dpkt.tcp.TCP:
					tcp_segment = ip_packet.data
					proto = dpkt.tcp.TCP;
					srcPort = tcp_segment.sport
					dstPort = tcp_segment.dport
					tcpPkts += 1
					tcpBytes += eth.data.len
				elif type(ip_packet.data) == dpkt.udp.UDP:
					udp_packet = ip_packet.data
					srcPort = udp_packet.sport
					dstPort = udp_packet.dport
					proto = dpkt.udp.UDP;
					udpPkts += 1
					udpBytes += eth.data.len
				else:
					unsupported += 1
					otherPkts += 1
					otherBytes += eth.data.len
					continue
			else:
				unsupported += 1
				otherPkts += 1
				if type(eth.data) == dpkt.ethernet.Ethernet:
					otherBytes += eth.data.len
				else: 
					otherBytes += 28 # we consider only arp over ethernet as an exception to the reule ... TODO
				continue

			id = create_conn_id(srcIP, dstIP, srcPort, dstPort, proto)
			create_new = True
			if id in connections:
				# check how old connection has already seen a timeout
				# TODO: make timeout configurable
				timeout = 300 # 5 minutes

				conn = connections[id]

				if conn.lastTs < (ts - timeout):
					print "timeout in " + conn.connString + " after " + str(conn.numPkts) + " packets",
					print "old: " + str(conn.lastTs) + " new: " + str(ts)
					dump_conn_to_result_db(conn, options, (withGaps, flowCollection, lowThroughput))
					del connections[id]
				elif proto == dpkt.tcp.TCP:
					# if we are not in a timeout stage, check if the connection
					# had been shut down. 
					# TODO should we have this instead the timeout checkings? Or instaed? Or should we have different timeouts for TCP? 
					if not conn.seqAnalyzer.isRunning():
						create_new = True
						del connections[id]
				else:
					create_new = False				
					
			if create_new:
				pString = "don't care"
				if proto == dpkt.tcp.TCP:
					pString = "TCP"
				elif proto == dpkt.udp.UDP:
					pString = "UDP"
				elif proto == dpkt.icmp.ICMP:
					pString = "ICMP"
				myConn = "%s:%u <-> %s:%u (%s)" % (binary_to_str(srcIP), srcPort, binary_to_str(dstIP), dstPort, pString)
				#print "New Connection: ", myConn
				connections[id] = ConnectionRecord(myConn, ts, srcIP, dstIP, srcPort, dstPort, proto, pString)
			connections[id].packet_seen(ts, buf, eth)


			if seen % 100000 == 0:
				progressOutput.write("\tread " + str(seen) + " packets ...\n")
	except Exception as inst:
		# end of file or bad file ending
		# ignore and continue
		progressOutput.write("Caught exception: %s\n" % (inst))
		import traceback
		traceback.print_exc(file=progressOutput)

	
	progressOutput.write("seen packets: %d\nunsupported packets (non IP, non UDP/TCP): %d\n" % (seen, unsupported))
	progressOutput.write("creating statistics files ...\n")

	# find connections that have a maximum gap larger than the specified one
	for c in connections:
		conn = connections[c]
		dump_conn_to_result_db(conn, options, (withGaps, flowCollection, lowThroughput))

	#print "Identified connections with long gaps. Now dumping pcap files that contain those connections. This can take a while ..."
	#dump_pcap_conns_with_gaps(gapConnections, options.inputFile)

if __name__ == "__main__":
	parser = OptionParser("usage: %prog [options]")
	parser.add_option('-i', '--input', dest="inputFile",
			  help = "input pcap file (required)")
	parser.add_option('-o', '--outputDir', dest="outputDir",
			  help = "output directory (required). Attention: All previous results in that directory will be overwritten!")
	parser.add_option('-f', '--filter', dest="pcapFilter",
			  help = "pcap filter (not implemented yet. please filter your file with tcpdump :()")
	parser.add_option('-m', '--max-gap', dest="maxGap", default=300, type="float",
			  help = "maximum gap between packets in seconds (default = 300 seconds)")
	parser.add_option('-p', '--min-throughput', dest="minThroughput", default = 10000001, type="float",
			  help = "minimum throughput")
	parser.add_option('-l', '--min-len', dest="minLenThroughput", type="int", default="1000000",
			  help = "min number of bytes for connections to be considered for throughput dumping")
#	parser.add_option('-g', '--gnuplot-path', dest="gnuplot_path", default="/usr/bin/gnuplot",
#			  help="path to gnuplot executable")

	(options, args) = parser.parse_args()

	if options.inputFile == None:
		print "ERROR: Did not get an input pcap file!"
		parser.print_help()
		sys.exit(-1)

	if options.outputDir == None:
		print "ERROR: Did not get an output directory!"
		parser.print_help()
		sys.exit(-1)
	
	# try to connect to mongodb
	try:
	        dst_conn = pymongo.Connection("127.0.0.1", 27017)
	except pymongo.errors.AutoReconnect, e:
		print >> sys.stderr, "Could not connect to MongoDB database!"
		sys.exit(1)
	# delete old results and create new collections
	dst_conn.drop_database("pcap")
	dst_db = dst_conn["pcap"]
	flowCollection = dst_db["all_flows"]
	lowThroughput = dst_db["low_throughput"]
	withGaps = dst_db["with_gaps"]
	pcapStats = dst_db["pcap_stats"]
	
	# open file for status output
	runningFilename = os.path.join(options.outputDir, "running_file.txt");
	runFile = open(runningFilename, 'w+')
	runFile.write("running\n")
	runFile.close()


	try:
		main(options, (flowCollection, lowThroughput, withGaps, pcapStats))
	except Exception, e:
		print e
	finally:
		# create indexes
		for col in (flowCollection, lowThroughput, withGaps):
			col.create_index("firstTs")
			col.create_index("pkts")
			col.create_index("bytes")

        	runFile = open(runningFilename, 'w+')
		runFile.write("finished\n")
		runFile.close()

