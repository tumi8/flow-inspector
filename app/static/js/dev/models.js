var AppState = Backbone.Model.extend({
	defaults: {
		page: "overview"
	}
});

var Flow = Backbone.Model.extend({
	parse: function(json) {
		// convert unix timestamp to JS Date
		json.bucket = new Date(json.bucket * 1000);
		return json;
	}
});

var BucketChartModel = Backbone.Model.extend({
	defaults: {
		value: "flows",
		interval: [],

		filterProtocols: "",
		filterProtocolsType: "inclusive",
		filterPorts: "",
		filterPortsType: "inclusive",
		filterIPs: "",
		filterIPsType: "inclusive"
	}
});

var HostViewModel = Backbone.Model.extend({
	defaults: {
		index: "nodes",
		value: "flows",
		fetchOnInit: false,
		limit: 15,
		bucket_size: null,
		interval: [],

		filterProtocols: "",
		filterProtocolsType: "inclusive",
		filterPorts: "",
		filterPortsType: "inclusive",
		filterIPs: "",
		filterIPsType: "inclusive"
	}
});


var DonutChartModel = Backbone.Model.extend({
	defaults: {
		index: "nodes",
		value: "flows",
		fetchOnInit: false,
		bucket_size: null,
		interval: [],
		limit: 30,

		filterProtocols: "",
		filterProtocolsType: "inclusive",
		filterPorts: "",
		filterPortsType: "inclusive",
		filterIPs: "",
		filterIPsType: "inclusive"
	}
});

var TimelineModel = Backbone.Model.extend({
	defaults: {
		value: "flows",
		interval: [],
		bucket_size: 0,
		color: null
	}
});

var QueryModel = Backbone.Model.extend({
	defaults: {
		startTime: null,
		endTime: null,
		ipAddress: null,
		maxLines: 10
	}
});

var GraphModel = Backbone.Model.extend({
	defaults: {
		nodeLimit: 255,
		showOthers: false,
		filterProtocols: "",
		filterProtocolsType: "inclusive",
		filterPorts: "",
		filterPortsType: "inclusive",
		filterIPs: "",
		filterIPsType: "inclusive"
	}
});

var FlowTableModel = Backbone.Model.extend({
	defaults: {
		flowLimit: 255,
		filterPorts: "",
		filterPortsType: "inclusive",
		value: ""
	}
});

var UndocumentedTableModel = Backbone.Model.extend({
	defaults: {
		entryLimit: 255
	}
});

var EdgeBundleModel = Backbone.Model.extend({
	defaults: {
		tension: 0.99,
		groupBytes: 0,
		nodeLimit: 50,
		hoverDirection: "outgoing",

		filterProtocols: "",
		filterProtocolsType: "inclusive",
		filterPorts: "",
		filterPortsType: "inclusive",
		filterIPs: "",
		filterIPsType: "inclusive"
	}
});

var HivePlotModel = Backbone.Model.extend({
	defaults: {
		axisScale: "linear",
		numericalValue: FlowInspector.COL_BYTES,
		mapAxis1: "",
		mapAxis2: "",
		mapAxis3: "0.0.0.0/0",
		directionAxis1: "both",
		directionAxis2: "both",
		directionAxis3: "both",

		filterProtocols: "",
		filterProtocolsType: "inclusive",
		filterPorts: "",
		filterPortsType: "inclusive",
		filterIPs: "",
		filterIPsType: "inclusive"
	}
});

var IndexEntry = Backbone.Model.extend({});

var UndocumentedIP = Backbone.Model.extend({});

var CachedCollection = Backbone.Collection.extend({
    sync: function(method, model, options) {
    	var that = this;
    	
    	// prevent caching with cached = false
    	if(!method === "read" || options.cached === false) {
    		return Backbone.sync.call(this, method, this, options);
    	}
    	
    	// init static cache
    	var cache = this.constructor._cache;
    	
    	// generate cache key
    	function hashCode(obj) {
    		var keys = [];
    		for(var k in obj) {
    			if(obj.hasOwnProperty(k)) {
    				var v = obj[k];
    				if(typeof(v) === "function") {
    					continue;
    				}
    				if(!Array.isArray(v) && typeof(v) === "object") {
    					v = hashCode(v);
    				}
    				keys.push(k + ":(" + v + ")");
    			}
    		}
    		return keys.sort().toString();
    	}
    	var cache_key = hashCode({ 
    		url: _.isFunction(this.url) ? this.url() : this.url,
    		options: options
    	});

    	if(cache[cache_key]) {
    		var cache_obj = cache[cache_key];
    		// check if there is already a response
    		if(cache_obj.response) {
    			// we have to clone the response to prevent side effects
    			var cp_resp = JSON.parse(JSON.stringify(cache_obj.response[0]));
    			
    			// make callback async ALWAYS!
	    		// otherwise this could lead to intransparent side effects
	    		setTimeout(function() {
    				options.success.call(this, 
    					cp_resp, 
    					cache_obj.response[1],
    					cache_obj.response[2]);
    			}, 1);
    		}
    		// otherwise add callbacks to queue
    		else {
    			cache_obj.callbacks.push(options);
    		}
    		return cache_obj["return"];
    	}
    	
    	// new request
    	var new_cache_obj = {
    		callbacks: [options]
    	};
    	cache[cache_key] = new_cache_obj;
    	
    	var params = _.clone(options);
    	params.success = function(resp, status, xhr) {
    		var args = Array.prototype.slice.call(arguments);
    		new_cache_obj.response = args;
    		
    		for(var i in new_cache_obj.callbacks) {
    			// we need scope
    			(function() {
  					var callback = new_cache_obj.callbacks[i];
	    			// we have to deep copy the response to prevent side effects
	    			var cp_resp = JSON.parse(JSON.stringify(resp));
	    				
	    			// make callback async ALWAYS!
	    			// otherwise this could lead to intransparent side effects
	    			setTimeout(function() {
	    				callback.success.call(this, cp_resp, status, xhr);
	    			}, 1);
    			}());
    		}	
    		delete new_cache_obj.callbacks;
    		
    		// cache timeout
    		setTimeout(function() {
    			delete cache[cache_key];
    		}, that.cache_timeout * 1000);
    	};
    	params.error = function() {
    		// we do not cache errors!
    		var args = Array.prototype.slice.call(arguments);
    		for(var i in new_cache_obj.callbacks) {
    			if(new_cache_obj.callbacks[i].error) {
    				new_cache_obj.callbacks[i].error.apply(this, args);
    			}
    		}	
    		delete cache[cache_key];
    	};
    	
    	new_cache_obj["return"] = Backbone.sync.call(this, method, this, params);
    	return new_cache_obj["return"];
    }
}, {
    _cache: {}
});

var Flows = CachedCollection.extend({
    cache_timeout: 60*10,
    model: Flow,
    url: "/api/bucket/query",
    parse: function(response) {
    	this.bucket_size = response.bucket_size;
    	response.results.forEach(function(d) {
    		// convert unix timestamp to JS Date
    		d.bucket = new Date(d.bucket * 1000);
    	});
    	return response.results;
    }
});


var IndexQuery = CachedCollection.extend({
    cache_timeout: 60*10,
    model: IndexEntry,
    total_count: 0,
    initialize: function(models, options) {
    	this.index = options.index || "nodes";
    },
    url: function() {
    	return "/api/index/" + this.index;
    },
    parse: function(response) {
	this.totalCounter = response.totalCounter;
    	return response.results;
    }
});

var DynamicIndexQuery = CachedCollection.extend({
    cache_timeout: 60*10,
    model: IndexEntry,
    total_count: 0,
    initialize: function(models, options) {
    	this.index = options.index || "nodes";
    },
    url: function() {
    	return "/api/dynamic/index/" + this.index;
    },
    parse: function(response) {
	this.totalCounter = response.totalCounter;
    	return response.results;
    }
});


var UndocumentedIPs = CachedCollection.extend({
	cache_timeout: 60*10,
	model: UndocumentedIP,
	url: function() {
		return "/api/hostinfo/";
	},
	dataTypes: [ "IP", "LASTSEEN", "LASTINFOCHECK" ],
	dataDescription: [ "IP Address", "Last Flow Seen", "Last Check in HostInfoDB" ],
	parse: function(response) {
		response.results.forEach(function(d) {
			// convert unix timestamp to JS Date
			d.LASTSEEN = new Date(d.LASTSEEN * 1000);
			d.LASTINFOCHECK  = new Date(d.LASTINFOCHECK  * 1000);
		});
		return response.results;
	}
});

