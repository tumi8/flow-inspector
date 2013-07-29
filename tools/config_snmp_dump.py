import sys,os

sys.path.insert(0, os.path.join('../', 'lib'))
sys.path.insert(0, os.path.join('../', 'config'))
sys.path.insert(0, os.path.join('..', 'snmp'))

#defines the fields that should be included in the datasource
data_source_fields = { 
	"ifInDiscards" : ("interface_phy"),
	"ifOutDiscards" : ("interface_phy"),
	"ifInErrors" : ("interface_phy"),
	"ifOutErrors" : ("interface_phy"), 
#	"ifInUcastPkts" : ("interface_phy"),
#	"ifOutUcastPkts" : ("interface_phy"),
#	"ifInOctets" : ("interface_phy"),
#	"ifOutOctets" : ("interface_phy"), 
#	"ifInNUcastPkts" : ("interface_phy"), 
#	"ifOutNUcastPkts" : ("interface_phy"),
#	"ifOutQLen" : ("interface_phy"),
	"ifInUnknownProtos" : ("interface_phy"),

#	"ifInMulticastPkts" : ("ifXTable"),
#	"ifOutMulticastPkts" : ("ifXTable"), 
#	"ifInBroadcastPkts" : ("ifXTable"),
#	"ifOutBroadcastPkts" : ("ifXTable"), 
	"ifHCInOctets" : ("ifXTable"),
	"ifHCOutOctets" : ("ifXTable"), 
	"ifHCInMulticastPkts"  : ("ifXTable"),
	"ifHCOutMulticastPkts" : ("ifXTable"),
	"ifHCInUcastPkts" : ("ifXTable"),
	"ifHCOutUcastPkts" : ("ifXTable"), 
	"ifHCInBroadcastPkts" : ("ifXTable"),
	"ifHCOutBroadcastPkts" : ("ifXTable"),
	"cpmCPUTotal5minRev" : ("ciscoCpu"),
	"cpmCPUTotal1minRev" : ("ciscoCpu"),
	"cpmCPUTotal5secRev" : ("ciscoCpu"),
	"cpmCPUTotal5min" : ("ciscoCpu"),
	"cpmCPUTotal1min" : ("ciscoCpu"),
#	"ciscoMemoryPoolName": ("ciscoMemory"),
	"ciscoMemoryPoolUsed": ("ciscoMemory"),
	"ciscoMemoryPoolFree": ("ciscoMemory"),
}

interface_graph_configs = [
	("ifHCInOctets", "ifHCOutOctets", "Bits (64Bit)", "Bits per second", 8),
	("ifHCInOctets", "ifHCOutOctets", "Bytes (64Bit)", "Bytes", 1),
	("ifHCInUcastPkts", "ifHCOutUcastPkts", "Unicast Pkts (64Bit)", "Pkts", 1),
	("ifHCInMulticastPkts", "ifHCOutMulticastPkts", "Multicast Pkts (64Bit)", "Pkts", 1),
	("ifHCInBroadcastPkts", "ifHCOutBroadcastPkts", "Broadcast Pkts (64Bit)", "Pkts", 1),
	("ifInDiscards", "ifOutDiscards", "Discarded Pkts (32 Bit)", "Pkts", 1),
	("ifInErrors", "ifOutErrors", "Pkt Errors (32 Bit)", "Pkts", 1),
	#("ifInOctets", "ifOutOctets", "Bytes (32Bit)", "Bytes", 1),
	#("ifInUcastPkts", "ifOutUcastPkts", "Unicast Pkts (32 Bit)", "Pkts", 1),
	#("ifInNUcastPkts", "ifOutNUcastPkts", "NonUnicast Pkts (32 Bit)", "Pkts", 1),
 	#("ifInMulticastPkts", "ifOutMulticastPkts", "Multicast Pkts (32 Bit)", "Pkts", 1),
	#("ifInBroadcastPkts", "ifOutBroadcastPkts", "Broadcast Pkts (32 Bit)", "Pkts", 1),
]
 
cpu_graph_configs = [
	("cpmCPUTotal1minRev", "cpmCPUTotal1minRev", "% CPU Load (1min)", "CPU Load", 1),
	("cpmCPUTotal5minRev", "cpmCPUTotal5minRev", "% CPU Load (5min)", "CPU Load", 1),
]
 

mem_graph_configs = [
	("ciscoMemoryPoolFree", "ciscoMemoryPoolUsed", "Memory Usage", "Memory", 1),
]

rtt_graph_configs = [
	("RTT", "RTT", "RTT Timings", "RTT", 1),
]

graph_dict = {
	"interface_details": interface_graph_configs,
	"cpu_details": cpu_graph_configs,
	"memory_details": mem_graph_configs,
	"rtt": rtt_graph_configs,
}
