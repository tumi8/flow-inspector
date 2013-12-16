all: update-rras snmpwalk-worker onlinecheck

onlinecheck: onlinecheck.cpp
	g++ -O3 -o onlinecheck onlinecheck.cpp

snmpwalk-worker: snmpwalk-worker.cpp 
	g++ -O3 -o snmpwalk-worker snmpwalk-worker.cpp -I/usr/local/include -L/usr/local/lib -I/opt/data/software/include -L/opt/data/software/lib -lnetsnmp

update-rras: update-rras.c
	gcc -O3 -o update-rras update-rras.c -I/usr/local/include -L/usr/local/lib -I/Users/braun/code/tsdb/ -L/Users/braun/code/tsdb/ -lrrd -ltsdb -ldb
