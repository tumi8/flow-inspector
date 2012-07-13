#!/usr/bin/env python

import sys, os, struct 
import subprocess
from optparse import OptionParser


sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor', 'dpkt-1.7'))
import dpkt


#|#################### global 
connections = {}
IPADDR_BINARY = struct.Struct("!BBBB")

##################### classes

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
		self.SYNACKSeen = False

		self.SYNFromServ = False

	def nextPacket(self, ipPacket):
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
					self.cliSeq = tcpSegment.seq + 1
					#print "Seen SYN for ", self.connString
				else:
					print "Duplicate SYN in " + self.connString
					if self.cliSeq != tcpSegment.seq + 1:
						print "\tduplicate SYN has different sequence number. Old: %d | New: %d" % (self.cliSeq - 1, tcpSegment.seq)
				return

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
					print "Weird: Got SYN from server in " + self.connString
					self.SYNFromServ = True
				else:
					# syn ack
					if self.SYNACKSeen:
						print "Duplicate SYN/ACK in " + self.connString
						if self.servSeq != tcpSegment.seq + 1:
							print "\tduplicate SYN/ACK has different sequence number. Old: %d | New: %d" % (self.servSeq - 1, tcpSegment.seq)
							
					self.servSeq = tcpSegment.seq + 1
				return
			# normal packet
			segPayloadLen = ipPacket.len - (ipPacket.hl * 4 + tcpSegment.off * 4)
			if self.servSeq == 0:
				self.servSeq = tcpSegment.seq + segPayloadLen
				return

			if self.servSeq != tcpSegment.seq:
				self.unexpectServCli += 1

			self.servSeq = tcpSegment.seq + segPayloadLen

class ConnectionRecord:
	def __init__(self, connString, ts, src, dst, sPort, dPort, proto):
		self.firstTs = ts;
		self.lastTs = ts;
		self.connString = connString

		self.src = src
		self.dst = dst
		self.sPort = sPort
		self.dPort = dPort
		self.proto = proto

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
			self.seqAnalyzer.nextPacket(eth.data)

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

	def dump_stat(self, fd):
		# desired conn string len
		lenStr = 55

		fd.write(self.connString)
		for i in range(0, lenStr - len(self.connString)):
			fd.write(" ")
		fmtString = "\tPkts: %6d\tBytes: %10d\tMax Gap: %6f\tAvg Gap: %6f\tMedian Gap: %6f\tAvg Throughput: %6f" % (self.numPkts, self.numBytes, self.maxDiff, self.avgDiff, self.calcMedianDiff(), self.calcAvgThroughput())
		fd.write(fmtString)
		if self.seqAnalyzer:
			fmtString = "\tSeqCliServ: %5d\tSecServCli: %5d" % (self.seqAnalyzer.unexpectCliServ, self.seqAnalyzer.unexpectServCli)
			fd.write(fmtString)

		fd.write("\n")

##################### functions

# plot a single graph
def plot(title, y_label, in_path, out_path, x_range="", columns=[2]):
    i = 0
    plots = ""
    for c in columns:
        i += 1
        plots += "'{0}' using 1:{1} w lines ls {2}, ".format(in_path, c, i)
        #plots += "'{0}' using 1:{1} w linespoints ls {2}, ".format(in_path, c, i)
        #plots += "'{0}' using 1:{1} w lp, ".format(in_path, c)
        #plots += "'{0}' using 1:{1} smooth csplines, ".format(in_path, c)
    plots = plots[:-2]

    if x_range:
        x_range = "set xrange[{0}]\n".format(x_range)

    plot_generation = \
            "set title '{0}'\n" \
            "set xlabel 'UTC or relative time' offset 0.0,-1.0\n" \
            "set ylabel '{1}'\n" \
            "set key autotitle columnhead\n" \
            "set mxtics\n" \
            "set mytics\n" \
            "set grid\n" \
            "set xdata time\n" \
            "set timefmt '%s'\n" \
            "{2}" \
            "set yrange[0:]\n" \
            'set style line 80 lt 0\n' \
            'set style line 80 lt rgb "#808080"\n' \
            'set style line 81 lt 3  # dashed\n' \
            'set style line 81 lt rgb "#808080" lw 0.5\n' \
            'set grid back linestyle 81\n' \
            'set border 3 back linestyle 80\n' \
            'set xtics nomirror\n' \
            'set ytics nomirror\n' \
            'set style line 1 lt 1\n' \
            'set style line 2 lt 1\n' \
            'set style line 3 lt 1\n' \
            'set style line 4 lt 1\n' \
            'set style line 1 lt rgb "#A00000" lw 2 pt 7\n' \
            'set style line 2 lt rgb "#00A000" lw 2 pt 9\n' \
            'set style line 3 lt rgb "#5060D0" lw 2 pt 5\n' \
            'set style line 4 lt rgb "#F25900" lw 2 pt 13\n' \
            "set terminal png size 1200, 600 noenhanced font '/usr/share/fonts/truetype/adf/GilliusADF-Regular.otf'\n" \
            "set output '{3}'\n" \
            "plot {4}".format(title, y_label, x_range, out_path, plots)
            #"set log y\n" \

#    print plot_generation

    #gnuplot_path ="/opt/data/software/bin/gnuplot"  
    #gnuplot_path ="/usr/local/bin/gnuplot"
    gnuplot_path ="/usr/bin/gnuplot"
    p = subprocess.Popen([gnuplot_path], shell=False,
                         stdin=subprocess.PIPE)
    # for python 3 do this:
    #p.communicate(input=bytes(plot_generation, 'utf-8'))
    p.communicate(input=plot_generation)
    return out_path



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



def dump_pcap_conns_with_gaps(gapConnections, inputFile):
	# re-read the pcap file and create smaller dump files with one connection
	pcapFile = dpkt.pcap.Reader(open(inputFile))
	read = 0
	for ts, buf in pcapFile:
		read += 1
		eth = dpkt.ethernet.Ethernet(buf)
		if type(eth.data) == dpkt.ip.IP:
			ip_packet = eth.data
			if type(ip_packet.data) == dpkt.tcp.TCP or type(ip_packet.data) == dpkt.udp.UDP:
				tl = ip_packet.data
				id = create_conn_id(ip_packet.src, ip_packet.dst, tl.sport, tl.dport, type(ip_packet.data))
				if id in gapConnections:
					gapConnections[id].write(ts, buf)

				if read % 100000 == 0:
					print "\tchecked ", read, " packets. Still ", seen - read, " to go..." 

############################# main 
		

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
	parser.add_option('-p', '--min-throughput', dest="minThroughput", default = 10000000, type="float",
			  help = "minimum throughput")
	parser.add_option('-l', '--min-len', dest="minLenThroughput", type="int", default="1000000",
			  help = "min number of bytes for connections to be considered for throughput dumping")

	(options, args) = parser.parse_args()
	if options.inputFile == None:
		print "ERROR: Did not get an input pcap file!"
		parser.print_help()
		sys.exit(-1)

	if options.outputDir == None:
		print "ERROR: Did not get an output directory!"
		parser.print_help()
		sys.exit(-1)
	
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

	# create statistics file
	gapFilename = os.path.join(options.outputDir, "stat-gap.txt")
	allFilename = os.path.join(options.outputDir, "all-flows.txt")
	throughputFilename = os.path.join(options.outputDir, "low-throughput.txt")
	gapFile = open(gapFilename, 'w+')
	allFile = open(allFilename, 'w+')
	throughputFile = open(throughputFilename, 'w+')

	bwStatsFilename = os.path.join(options.outputDir, "throughput.txt") 
	bwStats = open(bwStatsFilename, 'w+')
	bwStats.write('timestamp\tpkts\tthroughput\ttcp:pkts\ttcp:throughput\tudp:pkts\tudp:throughput\tother:pkts\tother:throughput\n')
	
	print "starting to read packets ..."
	unsupported = 0
	seen = 0

	lastSec = 0
	pkts = 0
	bytes = 0
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
				bwStats.write("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" % (lastSec, pkts, bytes * 8, tcpPkts, tcpBytes * 8, udpPkts, udpBytes * 8, otherPkts, otherBytes * 8))
				lastSec = ts
				pkts = bytes = tcpPkts = tcpBytes = udpPkts = udpBytes = otherPkts = otherBytes = 0
	
			seen += 1
			eth = dpkt.ethernet.Ethernet(buf)
			srcIP = 0
			dstIP = 0
			srcPort = 0
			dstPort = 0
			proto = ""

			pkts += 1
			if type(eth.data) == dpkt.ethernet.Ethernet:
				bytes += eth.data.len
			else:
				bytes += 28 # we only consider standard arp over ethernet ... TODO

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
					udpBytes += 1
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
			if id in connections:
				connections[id].packet_seen(ts, buf, eth)
			else:
				pString = "don't care"
				if proto == dpkt.tcp.TCP:
					pString = "TCP"
				elif proto == dpkt.udp.UDP:
					pString = "UDP"
				elif proto == dpkt.icmp.icmp:
					pString = "ICMP"
				myConn = "%s:%d <-> %s:%d (%s)" % (binary_to_str(srcIP), srcPort, binary_to_str(dstIP), dstPort, pString)
				#print "New Connection: ", myConn
				connections[id] = ConnectionRecord(myConn, ts, srcIP, dstIP, srcPort, dstPort, proto)
				connections[id].packet_seen(ts, buf, eth)


			if seen % 100000 == 0:
				print "\tread ", seen, " packets ..."
	except Exception as inst:
		# end of file or bad file ending
		# ignore and continue
		print "Caught exception: %s" % (inst)
		import traceback
		traceback.print_exc(file=sys.stdout)

	
	print "seen packets: %d\nunsupported packets (non IP, non UDP/TCP): %d" % (seen, unsupported)

	print "plotting throughput graphs ..."
	x_range = ""
	t = ""
	plot(t + "pps", "packet/s", bwStatsFilename, os.path.join(options.outputDir,  "pps.png"), x_range, [2,4,6,8])
    	plot(t + "throughput", "bit/s", bwStatsFilename, os.path.join(options.outputDir, "tp.png"), x_range, [3,5,7,9])
	

	print "creating statistics files ..."

	# find connections that have a maximum gap larger than the specified one
	gapConnections = {}
	for c in connections:
		conn = connections[c]
		if conn.maxDiff > options.maxGap:
			conn.dump_stat(gapFile)
			#conn.createWriter(os.path.join(options.outputDir, conn.connString + ".pcap"), pcapFile.snaplen);
			gapConnections[c] = conn
		conn.dump_stat(allFile)
		if conn.numBytes >= options.minLenThroughput:
			if conn.calcAvgThroughput() < options.minThroughput:
				conn.dump_stat(throughputFile)

	#print "Identified connections with long gaps. Now dumping pcap files that contain those connections. This can take a while ..."
	#dump_pcap_conns_with_gaps(gapConnections, options.inputFile)
			

