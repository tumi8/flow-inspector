#!/usr/bin/env python
# -*- coding: utf-8 -*-

# prepare paths
import sys
import os.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))


# include python modules
import argparse
import math
import datetime
import subprocess

# import other modules
import common
import backend
import config
import time

# prepare argument parser
# TODO: do something more meaningful here, right now the default values are mainly for the ease of testing
parser = argparse.ArgumentParser(description="Plot graph from snmp-data")
parser.add_argument("router", nargs="?", default="130.198.1.1", help="selects router from database")
parser.add_argument("interface", nargs="?", default="38", help="selects interface of router")
parser.add_argument("--timestamp_begin", default="1362024000", help="begin of data analysis")
parser.add_argument("--timestamp_end", default="1362099600", help="end of data analysis")
parser.add_argument("--outfile", default="test.png")
parser.add_argument("--backend", nargs="?", default=config.data_backend, help="Backend database type")
parser.add_argument("--dst-host", nargs="?", default=config.data_backend_host, help="Backend database host")
parser.add_argument("--dst-port", nargs="?", default=config.data_backend_port, type=int, help="Backend database port")
parser.add_argument("--dst-user", nargs="?", default=config.data_backend_user, help="Backend database user")
parser.add_argument("--dst-password", nargs="?", default=config.data_backend_password, help="Backend database password")
parser.add_argument("--dst-database", nargs="?", default=config.data_backend_name, help="Backend database name")
args = parser.parse_args()

# prepare database connection and create required collection objects
db = backend.databackend.getBackendObject(config.data_backend, config.data_backend_host, config.data_backend_port, config.data_backend_user, config.data_backend_password, config.data_backend_database)
interface_phy = db.getCollection("interface_phy")
ifxtable = db.getCollection("ifXTable")


# request data from database
# TODO: sqlbackend does not support AND-clauses... 'Solve' this by using case-sensitivity of the dictionary used (timestamp vs. TiMeStAmP)
#data_set = interface_phy.find({ "router": args.router, "ifIndex": args.interface,
#	"timestamp": {"$gt": args.timestamp_begin}, "TiMeStAmP": {"$lt": args.timestamp_end}},
#	sort={"timestamp": 1})

data_set = ifxtable.find({ "router": args.router, "if_number": args.interface,
	"timestamp": {"$gt": args.timestamp_begin}, "TiMeStAmP": {"$lt": args.timestamp_end}},
	sort={"timestamp": 1})


if len(data_set) == 0:
	print "Empty data set for router", args.router, "and interface", args.interface
	sys.exit(0)

# ----------------------------------------------------------------------------- #


# prepare data for processing

# here: convert absolut values into differential values, correct counter overruns and parse timestamps
# TODO: maye make a function out of this
values = []
last = data_set[0]["ifHCOutOctets"]
lastTimestamp = data_set[0]["timestamp"]
for record in data_set[1:]:
	if (last > record["ifHCOutOctets"]):
		value = 2**32 - last + record["ifHCOutOctets"]
	else:
		value = record["ifHCOutOctets"] - last
	if record["timestamp"] - lastTimestamp != 300:
		print "foo"
	lastTimestamp = record["timestamp"]
	values.append((datetime.datetime.fromtimestamp(record["timestamp"]).strftime("%m-%d_%H:%M:%S") , value))
	last = record["ifHCOutOctets"]
data_set = values
print >> sys.stderr, values


# ----------------------------------------------------------------------------- #

# do statistical calculations

# constants and intermediate results
# nomenclature
# x denotes the raw (and preprocessed) measurements
# s. denotes the smoothed value (i.e. ewma) of .
# p_ denotes the probability used for smoothing
# d. denotest the derivative of .
# prefixes are read right-to-left
# previous_. is used to store the value of . from previous iteration
sx = 0
p_sx = 0.125
sdsx = 0
p_sdsx = 0.125

previous_value = data_set[0][1]
previous_sx = previous_value
previous_dsx = 0
previous_sdsx = 0

# used for change detection
p_ewmv = 0.1
ewmv = 0
L = 3 
m0 = 0

# used to ensure significance between two turning points
average_height = 0
previous_height = 0
p_height = 0.125

# temporary file to be passed to gnuplot
# TODO: maybe use /tmp directory here
tmp_filename = "data.tmp." + str(os.getpid())
tmpfile = open(tmp_filename, "w")
firstline = True

# iterate over all lines in our data_set
# because we're calculating derivates we skip the first entry
for (descr, actual_value) in data_set[1:]:
	
	# do calculations
	value = actual_value
	sx = p_sx * value + (1-p_sx) * sx
	dsx = sx - previous_sx
	ddsx = dsx - previous_dsx
	sdsx = p_sdsx * dsx + (1 - p_sdsx) * sdsx
	dsdsx = sdsx - previous_sdsx
	
	# calculate variance and bounds
	ewmv = p_ewmv * (dsx- m0)**2 + (1 - p_ewmv) * ewmv
	low_bound = m0 - L * math.sqrt(ewmv * p_sx / (2-p_sx))
	high_bound = m0 + L * math.sqrt(ewmv * p_sx / (2-p_sx))

	# do change detection
	# we can't detect changes right now, we can only detect them in the next iteration
	# that's why we print the lins in two steps (see below)

	# print markers in graph
	# print NoV (NoValue) if no marker is desired here 
	low_value = "NoV"
	high_value = "NoV"

	# detect change of sign in dsx, detects minimal and maximum turning points
	# use copysign as python seems not to provide a sign function
	if (math.copysign(1, dsx) != math.copysign(1, previous_dsx)):
		# filter the turning points, display only those which are not too small compared to the average height between two turning points
		if (abs(previous_sx - previous_height) > 0.666 * average_height):
			low_value = previous_sx
		# update average height for every turning point, not only for those displayed
		# (otherwise detecting small series after a series of big changes doesn't work)
		average_height = p_height * abs(previous_sx - previous_height)  + (1 - p_height) * average_height
		previous_height = previous_sx

	# check whether sdsx is in bounds or not
	if (sdsx < low_bound) or (sdsx > high_bound):
		high_value = previous_sx

	# print low_value and high_value from previous iteration
	if firstline:
		firstline = False
	else:
		print >> tmpfile, low_value, high_value
	
	# print beginning of line, delay printing of markers as we can detect a change only in the next step
	print >> tmpfile, '"' + descr + '"', actual_value, sx, dsx, ddsx, sdsx, dsdsx, low_bound, high_bound,
	
	# set some values
	previous_value = actual_value
	previous_sx = sx
	previous_dsx = dsx
	previous_sdsx = sdsx
tmpfile.close()

# call gnuplot and create graph
gnuplot = subprocess.Popen(['gnuplot'], stdin=subprocess.PIPE)
print >> gnuplot.stdin, 'set terminal png size 2500,1200'
print >> gnuplot.stdin, 'set output "%s"' % args.outfile
print >> gnuplot.stdin, 'set xtics 5 rotate'
print >> gnuplot.stdin, 'set grid'
print >> gnuplot.stdin, 'set datafile missing "NoV"'
# print >> gnuplot.stdin, 'plot "data.tmp" using 0:3:xtic((int(column(0)) % 15 == 0) ? stringcolumn(1) : "") title "data" with lines linewidth 2, \\'
print >> gnuplot.stdin, 'plot "%s" using 0:2 title "data" with lines linewidth 2' % tmp_filename
#print >> gnuplot.stdin, '             "" using 3 title "sx" with lines linecolor rgb "green" linewidth 2, \\'
#print >> gnuplot.stdin, '             "" using 4 title "dsx" with lines linecolor rgb "blue" linewidth 2, \\'
#print >> gnuplot.stdin, '             "" using 5 title "ddsx" with lines linecolor rgb "orange" linewidth 2, \\'
#print >> gnuplot.stdin, '             "" using 10 title "marker" linecolor rgb "red", \\'
#print >> gnuplot.stdin, '             "" using 11 title "marker" linecolor rgb "red"'
print >> gnuplot.stdin, 'quit'

# wait for gnuplot to quit
gnuplot.wait()

# remove temporary file
os.remove(tmp_filename)

