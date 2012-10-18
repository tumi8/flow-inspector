if(!FlowInspector) {
	var FlowInspector = {};
}

// definitions
/*
FlowInspector.tcpColor = "rgba(204,0,0,1.0)"
FlowInspector.udpColor = "rgba(0,204,0,1.0)"
FlowInspector.icmpColor = "rgba(0,102,204,1.0)"
FlowInspector.otherColor = "rgba(255,163,71,1.0)"
*/
FlowInspector.tcpColor = "rgb(204,0,0)";
FlowInspector.udpColor = "rgb(0,204,0)";
FlowInspector.icmpColor = "rgb(0,102,204)";
FlowInspector.otherColor = "rgb(255,163,71)";

FlowInspector.COL_FIRST_SWITCHED = "flowStartSeconds"
FlowInspector.COL_LAST_SWITCHED = "flowEndSeconds"
// column names of IP addresses
FlowInspector.COL_SRC_IP = "sourceIPv4Address"
FlowInspector.COL_DST_IP = "destinationIPv4Address"
// column names of ports and protocol
FlowInspector.COL_SRC_PORT = "sourceTransportPort"
FlowInspector.COL_DST_PORT = "destinationTransportPort"
FlowInspector.COL_PROTO = "protocolIdentifier"
FlowInspector.COL_BUCKET = "bucket"


FlowInspector.COL_BYTES = "octetDeltaCount"
FlowInspector.COL_PKTS = "packetDeltaCount"
FlowInspector.COL_FLOWS = "flows"
FlowInspector.COL_ID = "id"

FlowInspector.COL_PROTO_TCP = "tcp"
FlowInspector.COL_PROTO_UDP = "udp"
FlowInspector.COL_PROTO_ICMP = "icmp"
FlowInspector.COL_PROTO_OTHER = "other"



/**
 * Transforms a 32bit IPv4 address into a human readable format
 * (e.g. 192.168.0.1)
 */
FlowInspector.ipToStr = function(ip) {
	return (ip >>> 24) + "." +
		   (ip >> 16 & 0xFF) + "." +
		   (ip >> 8 & 0xFF) + "." +
		   (ip & 0xFF);
};

/**
 * Transforms a human readable IPv4 address into a 32bit integer
 * (e.g. 192.168.0.1)
 */
FlowInspector.strToIp = function(str) {
	var parts = str.split(".");
	if(parts.length !== 4) {
		return false;
	}
	
	var ip = 0;
	for(var i = 0; i < 4; i++) {
		var j = parseInt(parts[i]);
		// check for range and Nan
		if(j !== j || j < 0 || j > 255) {
			return false;
		}
		ip = (ip << 8) + j;
	}
	return ip;
};

/**
 * Functions to work with Hilbert Curves.
 * (http://en.wikipedia.org/wiki/Hilbert_curve)
 */
 
//convert (x,y) to d
FlowInspector.hilbertXY2D = function(n, x, y) {
    var rx, ry, s, r, d = 0;
    for(s = n/2; s > 0; s /= 2) {
        rx = (x & s) > 0;
        ry = (y & s) > 0;
        d += s * s * ((3 * rx) ^ ry);
        r = FlowInspector.hilbertRot(s, x, y, rx, ry);
        x = r.x;
        y = r.y;
    }
    return d;
};
 
//convert d to (x,y)
FlowInspector.hilbertD2XY = function(n, d) {
    var rx, ry, s, r, t = d;
    var x = 0, y = 0;
    for(s = 1; s < n; s *= 2) {
        rx = 1 & (t/2);
        ry = 1 & (t ^ rx);
        r = FlowInspector.hilbertRot(s, x, y, rx, ry);
        x = r.x;
        y = r.y;
        x += s * rx;
        y += s * ry;
        t /= 4;
    }
    return { x: x, y: y };
};
 
//rotate/flip a quadrant appropriately
FlowInspector.hilbertRot = function(n, x, y, rx, ry) {
    var t;
    if(ry == 0) {
        if(rx == 1) {
            x = n-1 - x;
            y = n-1 - y;
        }
        t  = x;
        x = y;
        y = t;
    }
    return { x: x, y: y };
};

FlowInspector.isIPValid = function(ipaddr)  {
	// remove any spaces
	ipaddr = ipaddr.replace( /\s/g, "");
	// check for ipv4 address and optional subnet mask
	var re = /^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(\/\d{1,2})?$/;

	if (re.test(ipaddr)) {
		var  mask, parts;
		// get address and subnet mask
		parts = ipaddr.split("/");
		ipaddr = parts[0];

		if (parts[1] != "") {
			// if mask has been given, check if it is a valid mask
			mask = parseInt(parts[1], 10);
			if (mask == NaN || mask < 0 || mask > 32) {
				 // not a valid subnet mask
				return false;
			}
		}
		// check if address is valid
		parts = ipaddr.split(".");
		for (var i = 0; i < parts.length; ++i) {
			var num = parseInt(parts[i]);
			if (parts[i] == NaN || parts[i] < 0 || parts[i] > 255) {
				return false
			}	
		}
		return true;
        }
	return false;
}

FlowInspector.getTitleFormat = function(value) {
	if(value === FlowInspector.COL_PKTS) {
		return function(d) { 
			var val = 0;
			// if get is a function, call it, otherwise take d's value
			if (typeof d.get == 'function') {
				val = d.get(value);
			} else {
				val = d;
			}
			var factor = 1;
			var unit = "";
			if (val >= 1000*1000) {
				factor = 1000 * 1000;
				unit = "m";
			} else if (val >= 1000) {
				factor = 1000;
				unit = "k";
			}
			return Math.floor(val/factor)+unit; };
	}
	if(value === FlowInspector.COL_BYTES) {
		return function(d) { 
			var val = 0;
			// if get is a function, call it, otherwise take d's value
			if (typeof d.get == 'function') {
				val = d.get(value);
			} else {
				val = d;
			}
			var factor = 1;
			var unit = "B"
			// bigger than terrabyte
			if (val > 1000*1000*1000*1000) {
				factor = 1000*1000*1000*1000;
				unit = "TB";
			} else if (val > 1000*1000*1000) {
				factor = 1000*1000*1000;
				unit = "GB";
			} else if (val > 1000*1000) {
				factor = 1000*1000;
				unit = "MB";
			} else if (val > 1000) {
				factor = 1000;
				unit = "kB";
			} else {
				return (d3.format("f"))(val) + unit;
			}

			return (d3.format(".2f"))(val/factor) + unit; };
	}
	return function(d) { 
		var val = 0;
		// if get is a function, call it, otherwise take d's value
		if (typeof d.get == 'function') {
			val = d.get(value);
		} else {
			val = d;
		}
		return Math.floor(val) + FlowInspector.COL_FLOWS };
}

