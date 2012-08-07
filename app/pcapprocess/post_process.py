#!/usr/bin/env python 

import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'config'))

import config

import pymongo
import argparse
import subprocess

# plot a single graph
def plot(title, y_label, in_path, out_path, x_range="", columns=[2], gnuplot_path='/opt/data/software/bin/gnuplot'):
    i = 0
    plots = ""
    for c in columns:
        i += 1
        plots += "'{0}' using 1:{1} w points ls {2}, ".format(in_path, c, i)
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
            "set terminal svg size 2200, 800\n" \
            "set output '{3}'\n" \
            "plot {4}".format(title, y_label, x_range, out_path, plots)
            #"set log y\n" \

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

	# create filter
	filter = {}
	#filter[] = "130.176.17.140"
	filter["$or"] = [
		{ "srcIP": "%s" % args.ip[0] },
		{ "dstIP": "%s" % args.ip[0] }
	]

	print filter
	#filter["synFromServer"] = True

	flows = flowCollection.find(filter)
	flights = None

	for flow in flows:
		print "******************************"
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
				print field, ": ", flow[field]
			else:
				flights = flow[field]

		if len(flights) > 2:
			filename = os.path.join("svgs", "%s:%u_%s:%u_%s.dat" % (srcIP, srcPort, dstIP, dstPort, proto))
			f = open(filename, 'w+')
			f.write("flight_number start end duration pkts bytes\n")
			id = 1
			for chunk in flights:
				f.write("%u %f %f %f %u %u\n" % (id, chunk["start"], chunk["end"], chunk["end"] - chunk["start"], chunk["pkts"], chunk["bytes"]))
				id += 1
			f.close()
			plot("Throuphput flights", "flight no", filename, filename + ".svg", columns=["($4/$6)"])
			os.unlink(filename)

