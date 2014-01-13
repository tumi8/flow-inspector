CFLAGS=-I/usr/local/include -I/opt/data/software/include -I/Users/braun/code/tsdb/ -I/opt/data/sources/bap_dev/tsdb
LDFLAGS=-L/usr/local/lib -L/opt/data/software/lib -L/Users/braun/code/tsdb/ -L/opt/data/sources/bap_dev/tsdb/

all: update-rras snmpwalk-worker onlinecheck extract_tsdb extract_ip_info_from_nfdump rtt_measurement

onlinecheck: onlinecheck.cpp
	g++ -O3 -o onlinecheck onlinecheck.cpp

snmpwalk-worker: snmpwalk-worker.cpp 
	g++ -O3 -o snmpwalk-worker snmpwalk-worker.cpp $(CFLAGS) $(LDFLAGS) -lnetsnmp

update-rras: update-rras.c
	gcc -O3 -o update-rras update-rras.c $(CFLAGS) $(LDFLAGS) -lrrd -ltsdb -ldb-5.3

extract_tsdb: extract_tsdb.c
	gcc -O3 -o extract_tsdb extract_tsdb.c $(CFLAGS) $(LDFLAGS) -lrrd -ltsdb -ldb-5.3

extract_ip_info_from_nfdump: extract_ip_info_from_nfdump.c longest-prefix/tree.c longest-prefix/tree.h
	gcc -O3 -o extract_ip_info_from_nfdump extract_ip_info_from_nfdump.c longest-prefix/tree.c $(CFLAGS) $(LDFLAGS) -lrrd -ltsdb -ldb-5.3

rtt_measurement: rtt_measurement.cpp
	g++ -O3 -o rtt_measurement rtt_measurement.cpp $(CFLAGS) $(LDFLAGS) -ltsdb -ldb-5.3
