#!/bin/bash

if [ $# != 1 ]; then
	echo "Usage: $0  <rrd_dir>"
	exit 1
fi

rrd_dir=$1

function calc_rtt {
	if [ `uname` == "Darwin" ];
	then
		echo $(ping -t 3 -W 200 -c 4 $1 2>/dev/null | grep 'min/avg/max' | awk '{print$4}' | sed 's!/! !g' | awk '{print$2}')
	else
		echo $(ping -W 3 -c 4 $1 2>/dev/null | grep 'min/avg/max' | awk '{print$4}' | sed 's!/! !g' | awk '{print$2}')
	fi
}


function do_rtt_measurement {
	counter=0
	while read i
	do
	#for i in $(cat $measurement_file); do 
		location=`echo $i | awk '{print $1}'`
		host=`echo $i | awk '{print $2}'`
		#echo "`date`: checking $host ..."
		(
		current_time=`date +%s`
		avg_rtt=$(calc_rtt $host)
		if [ "X$avg_rtt" == "X" ]; then
			# no proper RTT measurement possible
			# discard value
			echo "$current_time $location -1"
			echo `date`": No RTT measurement for $host possible" 1>&2
			exit
		fi
		echo "$current_time $location $avg_rtt"

		rrd_file=$rrd_dir/rtt_$location.rrd
		# create rrd if not exists
		if [ ! -f $rrd_file ]; then
			let starttime=$current_time-1
			rrdtool create $rrd_file --start $starttime --step 60 DS:RTT:GAUGE:600:U:U RRA:AVERAGE:0.5:1:500  RRA:AVERAGE:0.5:1:600  RRA:AVERAGE:0.5:6:700  RRA:AVERAGE:0.5:24:775  RRA:AVERAGE:0.5:288:797  RRA:MAX:0.5:1:500  RRA:MAX:0.5:1:600  RRA:MAX:0.5:6:700  RRA:MAX:0.5:24:775  RRA:MAX:0.5:288:797
			if [ $? != 0 ]; then
				echo "`date`: failed to create rrd $rrd_file" 1>&1
				exit
			fi
		fi
		# update rrd
		rrdtool update $rrd_file --template "RTT" $current_time:$avg_rtt
		if [ $? != 0 ]; then
			echo "`date`: failed to update rrd $rrd_file" 1>&2
			exit
		fi

		#echo "RTT for $host: $avg_rtt ms"
		) &
		let counter=$counter+1
		if [ $counter -eq 200 ]; then
			wait
			counter=0
		fi
	done 
	wait
}

do_rtt_measurement
wait
