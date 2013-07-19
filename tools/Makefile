all: update-rras snmpwalk-worker

snmpwalk-worker: snmpwalk-worker.cpp 
	g++ -O3 -o snmpwalk-worker snmpwalk-worker.cpp -I/usr/local/include -L/usr/local/lib -I/opt/data/software/include -L/opt/data/software/lib -lnetsnmp

update-rras: update-rras.c
	gcc -O3 -o update-rras update-rras.c -I/usr/local/include -L/usr/local/lib -lrrd
