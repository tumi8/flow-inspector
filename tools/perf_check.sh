#!/bin/bash

from=1356998400
to=1359590400

let i=$from

while [ "$i" -lt "$to" ] 
do 
	(time ./update-rras snmp_dump_tmp_file.tmp rra/ $i >/dev/null 2>/dev/null) 2>&1| grep real | awk '{print $2}'
	let i=$i+300
done
