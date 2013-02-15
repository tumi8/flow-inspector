# parsing function for oid values

def plain(value):
	""" do nothing function """
	return value


def hex2ip(value):
	""" convert hex to ip (i.e. DE AD BE EF -> 222.173.190.239) """
	value = value.strip(" ")
	return '.'.join(str(int(part, 16))
		for part in [value[0:2], value[3:5], value[6:8], value[9:11]])


def netmask2int(netmask):
	""" convert netmask to int (i.e. 255.255.255.0 -> 24) """
	tmp = ''
	for part in netmask.split("."):
		tmp = tmp + str(bin(int(part)))
	return tmp.count("1")


def int2netmask(value):
	""" convert int to netmask (i.e. 24 -> 255.255.255.0) """
	value = '1' * int(value) + '0' * (32 - int(value))
	return '.'.join(str(int(part, 2))
		for part in [value[0:8], value[8:16], value[16:24], value[24:32]])


def ip2int(ip):
	""" convert ip to int """
	ip = ip.split('.')
	return (int(ip[0]) * (2 ** 24) + int(ip[1]) * (2 ** 16) +
		int(ip[2]) * (2 ** 8) + int(ip[3]))


def int2ip(i):
	""" convert int to ip """
	return (str(i // (2 ** 24)) + "." + str((i // (2 ** 16)) % 256) + "." +
		str((i // (2 ** 8)) % 256) + "." + str(i % 256))

def hex2ip2int(value):
	return ip2int(hex2ip(value))

def calc_ip_range(ip, mask):
	""" calculate smallest and biggest ip belonging to given network """
	
	mask_inv = 32 - int(mask)

	# strip network bits
	bits = int(ip) >> mask_inv

	# calculate network address by shifting network bits to left
	# efficitvely zeros the '(32-mask)'-right bits of ip
	low_ip = bits << mask_inv

	# calculate broadcast address by filling 'mask'-right bits with 1
	high_ip = low_ip + (2**mask_inv - 1)

	return (low_ip, high_ip)
