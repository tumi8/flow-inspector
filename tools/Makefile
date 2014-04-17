CFLAGS=-I/usr/local/include -I/opt/data/software/include -I/Users/braun/code/tsdb/ -I/opt/data/sources/bap_dev/tsdb
LDFLAGS=-L/usr/local/lib -L/opt/data/software/lib -L/Users/braun/code/tsdb/ -L/opt/data/sources/bap_dev/tsdb/

all: update-rras snmpwalk-worker onlinecheck 

onlinecheck: onlinecheck.cpp
	g++ -O3 -o onlinecheck onlinecheck.cpp

snmpwalk-worker: snmpwalk-worker.cpp 
	g++ -O3 -o snmpwalk-worker snmpwalk-worker.cpp $(CFLAGS) $(LDFLAGS) -lnetsnmp

update-rras: update-rras.c
	gcc -O3 -o update-rras update-rras.c $(CFLAGS) $(LDFLAGS) -lrrd 


