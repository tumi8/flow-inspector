#!/usr/bin/env python

import sys, os, struct 
import subprocess
from optparse import OptionParser
import pymongo
import bson


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
	def __init__(self, cli, cliPort, serv, servPort, connString):
		self.cli = cli
		self.serv = serv
		self.cliPort = cliPort
		self.servPort = servPort
		self.connString = connString
		self.lastTs = 0

		self.dataFlights = []
		self.currentFlight = None

		# next expected client sequence number
		self.seq = 0

		self.SYNSeen = False
		self.lastSYNSeen = 0
		self.lastSYNACKSeen = 0
		self.SYNACKSeen = False

		self.RSTSeen = False
		self.FINSeen = False

	def isRunning(self, ts):
		# try to estimate if a TCP connection can still expect more packets
		if self.RSTSeen:
			#print self.connString + " self.servRSTSeen: " + str(self.servRSTSeen), " self.cliRSTSeen: " + str(self.cliRSTSeen)
			return False
		if self.FINSeen and (self.lastTs - ts) > 20:
			#print self.connString + " self.servFINSeen: " + str(self.servFINSeen), " self.cliFINSeen: " + str(self.cliFINSeen)
			return False

		# be conservative (we need to think about this one here ... TODO, FIXME)
		return True

	def nextPacket(self, ipPacket, timestamp):
		if type(ipPacket.data) != dpkt.tcp.TCP:
			print "SequenceNumberAnalyzer got non TCP packet!"
			return

		self.lastTs = timestamp
		tcpSegment = ipPacket.data
		src = ipPacket.src
		dst = ipPacket.dst
		srcPort = tcpSegment.sport
		dstPort = tcpSegment.dport

		if tcpSegment.flags & dpkt.tcp.TH_SYN:
			if tcpSegment.flags & dpkt.tcp.TH_ACK:
				print "SYN/ACK seen in " + self.connString
				if self.SYNACKSeen:
					print "Duplicate SYN/ACK in " + self.connSTring + ".Old ts: " + str(self.lastSYNACKSeen) + ", new ts: " + str(timestamp)
				self.SYNACKSeen = True
				self.lastSYNACKSeen = timestamp
			else:
				print "SYN seen in ", self.connString
				if self.SYNSeen:
					print "Duplicate SYN in " + self.connString + ". Old ts: " + str(self.lastSYNSeen) + ", new ts: " + str(timestamp)
					if self.seq != tcpSegment.seq + 1:
						print "\tduplicate SYN has different sequence number. Old: %d | New: %d" % (self.seq - 1, tcpSegment.seq)
				self.lastSYNSeen = timestamp
				self.seq = tcpSegment.seq + 1
				self.SYNSeen = True

		# check for RST and FIN flags
		if tcpSegment.flags & dpkt.tcp.TH_RST:
			if self.FINSeen:
				print "Reset after FIN in " + self.connString
				pass
			else:
				print "Reset without FIN in " + self.connString + " at " + str(timestamp)
				pass
			self.RSTSeen = True

		if tcpSegment.flags & dpkt.tcp.TH_FIN:
			if self.FINSeen:
				print "Duplicate FIN in " + self.connString + " at " + str(timestamp)
				pass
			else:
				print "FIN seen in " + self.connString
				pass
			self.FINSeen = True

		# normal packet. 
		#print "Stats: ", ipPacket.len, ipPacket.hl * 4, tcpSegment.off * 4, "(", self.connString, ")"
		segPayloadLen = ipPacket.len - (ipPacket.hl * 4 + tcpSegment.off * 4)
		if segPayloadLen == 0:
			# do we have a running flight?
			# if so, we consider it finished
			if self.currentFlight:
				# finish the flight and store it
				self.dataFlights.append(self.currentFlight)
				self.currentFlight = None

			# fin packets add one to the sequence number
			if tcpSegment.flags & dpkt.tcp.TH_FIN:
				segPayloadLen = 1
		else:
			# add data to the current flight or create new flight
			if not self.currentFlight:
				self.currentFlight = dict()
				self.currentFlight["start"] = timestamp
				self.currentFlight["pkts"] = 0
				self.currentFlight["bytes"] = 0

			self.currentFlight["end"] = timestamp
			self.currentFlight["pkts"] += 1
			self.currentFlight["bytes"] += segPayloadLen

		# check if we received a SYN
		if self.seq == 0:
			#print "In %s: Expected seq: %d. Got seq: %d, Diff: %d, Payload len: %d" % (self.connString, self.seq, tcpSegment.seq, self.seq - tcpSegment.seq, segPayloadLen)
			self.seq = tcpSegment.seq + segPayloadLen
			return

		if self.seq != tcpSegment.seq:
			#print "In %s: Expected seq: %d. Got seq: %d, Diff: %d, Payload len: %d" % (self.connString, self.seq, tcpSegment.seq, self.seq - tcpSegment.seq, segPayloadLen)
			pass
		else:
			#print "In %s: Good     seq: %d, Got seq: %d, Diff: %d, Payload len: %d" % (self.connString, self.seq, tcpSegment.seq, self.seq - tcpSegment.seq, segPayloadLen)
			pass
		self.seq = tcpSegment.seq + segPayloadLen
		#if not tcpSegment.seq == self.cliSeq
			
class ConnectionRecord:
	def __init__(self, connString, ts, src, dst, sPort, dPort, proto, pString, id):
		self.firstTs = ts;
		self.lastTs = ts;
		self.connString = connString

		self.src = src
		self.dst = dst
		self.sPort = sPort
		self.dPort = dPort
		self.proto = proto
		self.pString = pString

		self.id = id

		self.numPkts = 0
		self.numBytes = 0

		self.pktInfo = []

		self.maxDiff = 0;
		self.diffs = [];
		self.avgDiff = 0;

		self.writer = None
		
		if proto == dpkt.tcp.TCP:
			self.seqAnalyzer = SequenceNumberAnalyzer(src, sPort, dst, dPort, self.connString)
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

		#self.pktInfo.append({"ts": ts, "len": eth.data.len})

		if self.seqAnalyzer:
			self.seqAnalyzer.nextPacket(eth.data, ts)

	def calcAvgThroughput(self):
		if (self.lastTs - self.firstTs) == 0:
			return 0
		return (float(self.numBytes) / float((self.lastTs - self.firstTs))) * 8.0;

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
		# convert decimal.Decimal to float before storing it to mongo
		firstTs = round(self.firstTs, 6)
		lastTs = round(self.lastTs, 6)
		doc = { "_id": bson.binary.Binary(str(self.firstTs) + self.id), "firstSwitched": firstTs, "lastSwitched": lastTs, "srcIP": binary_to_str(self.src), "dstIP": binary_to_str(self.dst), "srcPort": self.sPort, "dstPort": self.dPort, "proto": self.pString, "pkts": self.numPkts, "bytes": self.numBytes, "maxDiff": round(self.maxDiff, 6), "avgDiff": round(self.avgDiff, 6), "medianDiff": round(self.calcMedianDiff(), 6), "avgThroughput": round(self.calcAvgThroughput(), 6) }

		#doc['pktInfo' ] = self.pktInfo

		if self.proto == dpkt.tcp.TCP:
			doc['synSeen'] = self.seqAnalyzer.SYNSeen
			doc['synAckSeen'] = self.seqAnalyzer.SYNACKSeen
			if self.seqAnalyzer.currentFlight:
				self.seqAnalyzer.dataFlights.append(self.seqAnalyzer.currentFlight)
			doc['flights'] =  self.seqAnalyzer.dataFlights

		collection.insert(doc)

##################### functions

def require_new_conn(conn):
	if conn.proto == dpkt.tcp.TCP:
		# if we are not in a timeout stage, check if the connection
		# had been shut down. 
		# TODO should we have this instead the timeout checkings? Or instaed? Or should we have different timeouts for TCP? 
		if not conn.seqAnalyzer.isRunning(ts):
			return True
		elif tcpSegment.flags & dpkt.tcp.TH_SYN and not tcpSegment.flags & dpkt.tcp.TH_ACK:
			# check if we should really start a new connection on a syn
			# see if we already have seen a syn from the client (because of asymetric routes, we might have seen the SYN/ACK before the SYN)
			if not conn.seqAnalyzer.SYNSeen:
				# check for the reason why we didn't see the SYN (could be lost or it could
				# not have been seen because the monitoring started after the syn has been sent
				# check for timeout
				if conn.lastTs < (ts - timeout):
					# we haven't seen a packet in a while. assume a new connection
					print "Removing old connection because of new SYN packet ..."
					return True
				else:
					# we have recently seen new packets, and did not observe a shutdown.
					# so this is probably a late SYN due to asymetric routes
					return False
			else: 	
				print "Removing old connection because of new SYN packet ..."
				return True
		else:
			return False
	elif conn.lastTs < (ts - timeout):
		# we can only work with timeouts on the other protocols
		print "timeout in " + conn.connString + " after " + str(conn.numPkts) + " packets",
		print "old: " + str(conn.lastTs) + " new: " + str(ts)
		return True
	# in all other cases: resume the connection
	return False
	

def binary_to_str(binaddr):
	return "%d.%d.%d.%d" % IPADDR_BINARY.unpack(binaddr)

def create_conn_id(srcIP, dstIP, srcPort, dstPort, proto, biflow):
	if biflow:
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

def processPacket(ts, buf, collections, progressOutput):
	global unsupported, seen, lastSec, allPkts, allBytes, tcpPkts, tcpBytes, udpBytes, udpPkts, otherPkts, otherBytes, connections
	(flowCollection, lowThroughput, withGaps, pcapStats) = collections
	if lastSec == 0:
		lastSec = ts
	if int(ts) > lastSec:
		doc = {
			"second": int(lastSec),
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
			return 
	else:
		unsupported += 1
		otherPkts += 1
		if type(eth.data) == dpkt.ethernet.Ethernet:
			otherBytes += eth.data.len
		else: 
			otherBytes += 28 # we consider only arp over ethernet as an exception to the reule ... TODO
			return

	id = create_conn_id(srcIP, dstIP, srcPort, dstPort, proto, biflow=False)
	create_new = True
#	print ts, " ",  binary_to_str(srcIP), " ", srcPort, " ", binary_to_str(dstIP), " ", dstPort, " ", proto, " ", eth.data.len,
	if proto == dpkt.tcp.TCP:
		tcpSegment = ip_packet.data
#		if tcpSegment.flags & dpkt.tcp.TH_SYN:
#			print " SYN ",
#		if tcpSegment.flags & dpkt.tcp.TH_ACK:
#			print " ACK ",
#		if tcpSegment.flags & dpkt.tcp.TH_RST:
#			print " RST ",
#		if tcpSegment.flags & dpkt.tcp.TH_FIN:
#			print " FIN ",
#	print ""

	if id in connections:
		# check how old connection has already seen a timeout
		# TODO: make timeout configurable
		timeout = 300 # 10 minutes

		conn = connections[id]
		create_new = False

		if proto == dpkt.tcp.TCP:
			if tcpSegment.flags & dpkt.tcp.TH_SYN:
				if not conn.seqAnalyzer.isRunning(ts):
					print conn.connString, " is no longer running ..."
					create_new = True
				else:
					# TODO: check if we see a syn or syn/ack retransmit here ...
					print "Seen new SYN on existing connection without proper shutdown in " + conn.connString
					create_new = True
		else:
			if conn.lastTs < (ts - timeout):
				# we can only work with timeouts on the other protocols
       				print "timeout in " + conn.connString + " after " + str(conn.numPkts) + " packets",
				print "old: " + str(conn.lastTs) + " new: " + str(ts)
				create_new = True


		# create_new = require_new_conn(conn)
		if create_new:
			dump_conn_to_result_db(conn, options, (withGaps, flowCollection, lowThroughput))
			del connections[id]
			
	if create_new:
		# if we are to create a new connection record, make sure that if we see a SYN or SYN/ACK
		# packet, we make the appropriate end point server or client (due to asymetric routes, 
		# we can end up seeing the SYN/ACK packet before the SYN
		#if tcpSegment.flags & dpkt.tcp.TH_SYN and tcpSegment.flags & dpkt.tcp.TH_ACK:
		#	print "First packet on new connection is SYN/ACK ..."
			# swap client/server
		#	srcIP, dstIP = dstIP, srcIP
		#	srcPort, dstPort = dstPort, srcPort

		pString = "don't care"
		if proto == dpkt.tcp.TCP:
			pString = "TCP"
		elif proto == dpkt.udp.UDP:
			pString = "UDP"
		elif proto == dpkt.icmp.ICMP:
			pString = "ICMP"
		myConn = "%s:%u <-> %s:%u (%s)" % (binary_to_str(srcIP), srcPort, binary_to_str(dstIP), dstPort, pString)
		#print "created " + myConn
		#print "New Connection: ", myConn
		connections[id] = ConnectionRecord(myConn, ts, srcIP, dstIP, srcPort, dstPort, proto, pString, id)

	connections[id].packet_seen(ts, buf, eth)

	if seen % 100000 == 0:
		progressOutput.write("\tread " + str(seen) + " packets ...\n")
	


############################# main 
		


def main(options, args, collections):
	global unsupported, seen, lastSec, allPkts, allBytes, tcpPkts, tcpBytes, udpBytes, udpPkts, otherPkts, otherBytes, connections

	# open pcap files
	files = []
	if len(args) == 1 and args[0] == '-':
		files.append(sys.stdin)
	else:
		for file in args:
			print "Opening file \"%s\" for reading ..." % (file)
			f = open(file, 'r')
			files.append(f)


	pcapFiles = {}
	for f in files:
		pcapFiles[iter(dpkt.pcap.Reader(f))] = None


	if options.pcapFilter != None:
		print "Setting pcap filter: ", options.pcapFilter
		pcapFile.setfilter(options.pcapFilter)

	# create output directory if it does not exist
	if not os.access(options.outputDir, os.R_OK | os.W_OK):
		os.makedirs(options.outputDir)


	progressOutput = Unbuffered(open(os.path.join(options.outputDir, "analysis-output.txt"), 'w+'))

	progressOutput.write("starting to read packets ...\n")

	
	while len(pcapFiles) > 0:
		# read one packet from each pcap file. process the packet with the 
		# smallest timestamp
		for pcapFile in pcapFiles:
			if pcapFiles[pcapFile] == None:
				# read a new packet
				try:
					(ts, buf) = pcapFile.next()
					pcapFiles[pcapFile] = (ts, buf)
				except Exception as inst:
					print >> sys.stderr, "Could not read more from %s: %s" % (pcapFile, inst)
					del pcapFiles[pcapFile]
					break
		if len(pcapFiles) == 0:
			continue
		# we now have one packet from each file
		# pick the one with the oldest timestamp and process the packet
		oldestTs = 0
		oldestBuf = 0
		oldestPcap = None
		for file in pcapFiles:
			(ts, buf) = pcapFiles[file]
			if oldestTs == 0 or oldestTs > ts:
				oldestTs = ts
				oldestBuf = buf
				oldestPcap = file
		
		processPacket(oldestTs, oldestBuf, collections, progressOutput)
		# processed packet, remove it from the cache
		pcapFiles[oldestPcap] = None
	
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
		main(options, args, (flowCollection, lowThroughput, withGaps, pcapStats))
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

