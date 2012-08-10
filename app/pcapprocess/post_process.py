#!/usr/bin/env python 

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'config'))

import config

import pymongo
import argparse
import subprocess

def do_plot(plot_generation, gnuplot_path, out_path):
#    print plot_generation

    #gnuplot_path ="/opt/data/software/bin/gnuplot"  
    #gnuplot_path ="/usr/local/bin/gnuplot"
    #gnuplot_path ="/usr/bin/gnuplot"
    p = subprocess.Popen([gnuplot_path], shell=False,
                         stdin=subprocess.PIPE)
    # for python 3 do this:
    #p.communicate(input=bytes(plot_generation, 'utf-8'))
    p.communicate(input=plot_generation)
    return out_path

def plot_ports(title, y_label, in_path, out_path, x_range="", columns=[2, 3], gnuplot_path='/usr/local/bin/gnuplot'):
	i = 0
	plots = ""
	for c in columns:
		i += 1
		plots += "'{0}' using 1:{1} w boxes fs solid,".format(in_path, c)
		#plots += "'{0}' using 1:{1} w linespoints ls {2}, ".format(in_path, c, i)
		#plots += "'{0}' using 1:{1} w lp, ".format(in_path, c)
		#plots += "'{0}' using 1:{1} smooth csplines, ".format(in_path, c)
	plots = plots[:-2]
	if x_range:
		x_range = "set xrange[{0}]\n".format(x_range)
	plot_generation = \
		"set title '{0}'\n" \
		"set ylabel '{1}'\n" \
		"set grid\n" \
		"{2}" \
		"set terminal svg size 1200, 800\n" \
		"set output '{3}'\n" \
		"plot {4}".format(title, y_label, x_range, out_path, plots)
		#"set log y\n" \
	
	return do_plot(plot_generation, gnuplot_path, out_path)


def plot_hosts(title, y_label, in_path, out_path, x_range="", columns=[2, 3], gnuplot_path='/usr/local/bin/gnuplot'):
	i = 0
	plots = ""
	for c in columns:
		i += 1
		plots += "'{0}' using 1:{1} w points,".format(in_path, c)
		#plots += "'{0}' using 1:{1} w linespoints ls {2}, ".format(in_path, c, i)
		#plots += "'{0}' using 1:{1} w lp, ".format(in_path, c)
		#plots += "'{0}' using 1:{1} smooth csplines, ".format(in_path, c)
	plots = plots[:-2]
	if x_range:
		x_range = "set xrange[{0}]\n".format(x_range)
	plot_generation = \
		"set title '{0}'\n" \
		"set ylabel '{1}'\n" \
		"set ydata time\n" \
		"set timefmt '%s'\n" \
		"set nokey\n" \
		"set grid\n" \
		"{2}" \
		"set terminal svg size 1800, 1200\n" \
		"set output '{3}'\n" \
		"plot {4}".format(title, y_label, x_range, out_path, plots)
		#"set log y\n" \
	
	return do_plot(plot_generation, gnuplot_path, out_path)



# plot a single graph
def plot_general(title, y_label, in_path, out_path, x_range="", columns=[2], gnuplot_path='/usr/local/bin/gnuplot', imgType="png", plotType="lines"):
	i = 0
	plots = ""
	for c in columns:
		i += 1
		plots += "'{0}' using 1:{1} w {2} ls {3}, ".format(in_path, c, plotType, i)
		#plots += "'{0}' using 1:{1} w linespoints ls {2}, ".format(in_path, c, i)
		#plots += "'{0}' using 1:{1} w lp, ".format(in_path, c)
		#plots += "'{0}' using 1:{1} smooth csplines, ".format(in_path, c)
	plots = plots[:-2]
	if x_range:
		x_range = "set xrange[{0}]\n".format(x_range)
	plot_generation = \
		"set title '{0}'\n" \
		"set yrange[0:]\n" \
		"set key autotitle columnhead\n" \
		"set mxtics\n" \
		"set mytics\n" \
		"set xdata time\n" \
		"set timefmt '%s'\n" \
		"set ylabel '{1}'\n" \
		"set grid\n" \
		"set xlabel 'UTC or relative time' offset 0.0,-1.0\n" \
		"{2}" \
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
		"set terminal {5} size 2200, 800 enhanced font '/usr/share/fonts/truetype/adf/GilliusADF-Regular.otf'\n" \
		"set output '{3}'\n" \
		"plot {4}".format(title, y_label, x_range, out_path, plots, imgType)
		#"set log y\n" \
	return do_plot(plot_generation, gnuplot_path, out_path)


############################## globals ##########################


outputDir = "svgs"


############################## main #############################

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Import IPFIX flows from Redis cache into MongoDB.")
	parser.add_argument("--host", nargs="?", default=config.db_host, help="MongoDB host")
	parser.add_argument("--port", nargs="?", default=config.db_port, type=int, help="MongoDB port")
	parser.add_argument("ip", nargs=1, help="IP Address")

	args = parser.parse_args()


	# init pymongo connection
	try:
		dst_conn = pymongo.Connection(args.host, args.port)
	except pymongo.errors.AutoReconnect, e:
		print >> sys.stderr, "Could not connect to MongoDB database!"
		sys.exit(1)
	
	pcapDB = dst_conn["pcap"]
	flowCollection = pcapDB["all_flows"]

	analysisIP = args.ip[0]

	# create filter
	filter = {}
	#filter[] = "130.176.17.140"
	filter["$or"] = [
		{ "srcIP": {"$regex": analysisIP }}, 
		{ "dstIP": {"$regex": analysisIP }}
	]

	print filter
	#filter["synFromServer"] = True

	flows = flowCollection.find(filter)
	flights = None

	connFilename = os.path.join(outputDir, "conns-%s.dat" % (analysisIP))
	connFile = open(connFilename, "w+")
	connIDs = dict()
	connections = 0
	connectionCounter = 0

	portsDict = dict()

	pktFilename = os.path.join(outputDir, "pkts-%s.dat" % (analysisIP))
	pktFile = open(pktFilename, 'w+')

	concurrentConns = 0

	print "Iterating through flows ..."
	for flow in flows:
		#print "******************************"
		for field in flow:
			if field != "flights":
				if field == "srcIP":
					srcIP = flow[field]
				elif field == "dstIP":
					dstIP = flow[field]
				elif field == "srcPort":
					srcPort = flow[field]
				elif field == "dstPort":
					dstPort = flow[field]
				elif field == "proto":
					proto = "proto"
				#print field, ": ", flow[field]
			else:
				flights = flow[field]

		#for pkt in flow["pktInfo"]:
		#	pktFile.write("%f %u\n" % (pkt["ts"], pkt["len"]))

		if srcIP == args.ip[0]:
			sourceIP = srcIP
			sourcePort = srcPort
			connectedIP = dstIP
			destinationPort = dstPort
		else:
			sourceIP = dstIP
			sourcePort = dstPort
			connectedIP = srcIP
			destinationPort = srcPort

		if sourcePort in portsDict:
			(local, remote) = portsDict[sourcePort]
			portsDict[sourcePort] = (local + 1, remote)
		else: 
			portsDict[sourcePort] = (1, 0)
		if destinationPort in portsDict:
			(local, remote) = portsDict[destinationPort]
			portsDict[destinationPort] = (local, remote + 1)
		else:
			portsDict[destinationPort] = (0, 1)

		connID = sourceIP + connectedIP + str(sourcePort) + str(destinationPort)
		if connID in connIDs:
			dataPoint = connIDs[connID]
		else:
			dataPoint = connectionCounter
			connIDs[connID] = connectionCounter
			connectionCounter += 1
		if connections >= 0:
			connFile.write("%u %f %f\n" % (connectionCounter, flow["firstSwitched"], flow["lastSwitched"]))
			connections += 1

		if len(flights) > 200000:
			filename = os.path.join(outputDir, "%s:%u_%s:%u_%s.dat" % (srcIP, srcPort, dstIP, dstPort, proto))
			f = open(filename, 'w+')
			f.write("flight_number start end duration pkts bytes\n")
			id = 1
			for chunk in flights:
				f.write("%u %f %f %f %u %u\n" % (id, chunk["start"], chunk["end"], chunk["end"] - chunk["start"], chunk["pkts"], chunk["bytes"]))
				id += 1
			f.close()
			plot_general("Throughput for flights", "flight no", filename, filename + ".svg", columns=["($4/$6)"])
			os.unlink(filename)

	pktFile.close()
	plot_general("Packets", "Packet size", pktFilename, pktFilename + ".png", columns = [2], plotType="points")

	#os.unlink(pktFilename)

	connFile.close()
	plot_hosts("Connection durations", "conn time", connFilename, connFilename + ".svg")
	os.unlink(connFilename)

	# write the ports stuff
	print "Generating ports information ..."

	portsFilename = os.path.join(outputDir, "ports-%s.dat" % (analysisIP))
	portsFile = open(portsFilename, 'w+')
	portsFile.write("Port\tLocal\tRemote\n")

	for port in portsDict:
		(local, remote) = portsDict[port]
		portsFile.write("%u\t%u\t%u\n" % (port, local, remote))
	portsFile.close()
	plot_ports("Ports Distribution", "Connections to port", portsFilename, portsFilename + ".svg", columns=[2,3])
	os.unlink(portsFilename)

	# get throughput images
	print "Generating throughput data ..."

	pcapStatsFilename = os.path.join(outputDir, "throughput.dat")

	pcapStatsFile = open(pcapStatsFilename, "w+")
	pcapStatsFile.write('timestamp\tpkts\tthroughput\ttcp:pkts\ttcp:throughput\tudp:pkts\tudp:throughput\tother:pkts\tother:throughput\n')

	pcapStatsCollection = pcapDB["pcap_stats"]
	stats = pcapStatsCollection.find()
	for stat in stats:
		pcapStatsFile.write("%f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n" % (round(float(stat["second"]), 6), stat["allPkts"], stat["allBytes"] * 8, stat["tcpPkts"], stat["tcpBytes"] * 8, stat["udpPkts"], stat["udpBytes"] * 8, stat["otherPkts"], stat["otherBytes"] * 8))
	pcapStatsFile.close()

	plot_general("pps", "packet/s", pcapStatsFilename, os.path.join(outputDir,  "pps.png"), columns=[2,4,6,8])
	plot_general("throughput", "bit/s", pcapStatsFilename, os.path.join(outputDir, "tp.png"), columns=[3,5,7,9])
       
	os.unlink(pcapStatsFilename)
