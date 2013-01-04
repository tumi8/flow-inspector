var MapsView = Backbone.View.extend({
	className: "maps-view",
	events: {},
	initialize: function(options) {
		if(!this.model) {
			this.model = new MapsViewModel();
		}
    	
		this.model.bind("change:value", this.changeValue, this);
		this.model.bind("change:interval", this.changeInterval, this);
		this.model.bind("change:bucket_size", this.changeBucketSize, this);
    	
		this.loaderTemplate = _.template($("#loader-template").html());

    		this.markerList = new Array();
		var myLatlng = new google.maps.LatLng(48.1333, 11.5667);
		var marker = new google.maps.Marker({
			position: myLatlng,
			title: "MÃnchen!"
			});
		this.markerList.push(marker);

		this.index = options.index;
		this.index.bind("reset", this.getGeoInfo, this);

		this.geoInfoQuery = new GeoInfoQuery()
		this.geoInfoQuery.bind("reset", this.updateGeoCache, this);

		if (options.fetchEmptyInterval !== undefined) {
			this.model.set({fetchEmptyInterval : options.fetchEmptyInterval})
		}

		this.showLimit = this.model.get("limit") + 1;

		// fetch at the end because a cached request calls render immediately!
		if (this.model.get("fetchOnInit")) {
			this.fetchData();
		}
	},
	render: function() {
		var that = this;

		var mapOptions = {
			center: new google.maps.LatLng(48.1333, 11.5667),
			zoom : 2,
			mapTypeId : google.maps.MapTypeId.ROADMAP
		};

		this.map = new google.maps.Map(
			document.getElementById("map_canvas"),
			mapOptions);
		if (this.markerList.length > 0) {
			this.markerList.forEach(function(marker) {
				marker.setMap(that.map);
			});
		}
	},
	changeValue: function(model, value) {
		this.fetchData();
	},
	changeInterval: function(mode, value) {
		this.fetchData();
	},
	fetchData: function(model, value) {
		this.index.models = [];
		this.render();

		var fetchEmptyInterval = this.model.get("fetchEmptyInterval");
		var interval = this.model.get("interval");
		if (!fetchEmptyInterval && interval.length == 0) {
			return; 
		}

		var limit       = this.model.get("limit");
		var sortField   = this.model.get("value");
		var bucket_size = this.model.get("bucket_size");

		var data = {
			"limit": limit + 1,
			"sort": sortField + " desc"
		};

		if (interval.length > 0) {
			data["start_bucket"] =  Math.floor(interval[0].getTime() / 1000);
			data["end_bucket"] =  Math.floor(interval[1].getTime() / 1000);
		} else {
			data["start_bucket"] = 0;
			data["end_bucket"] = 0;
		}
		if (bucket_size) {
			data["bucket_size"] = bucket_size;
		}

		data = FlowInspector.addToFilter(data, this.model, FlowInspector.COL_IPADDRESS, false);
		if (data == null) {
			return;
		}

		this.index.fetch({data: data});
	},
	changeBucketSize: function() {
		this.fetchData();
	},
	getGeoInfo : function() {
		var data = {}
		if (this.index.length > 0) {
			var lookup_ips = "";
			this.index.each(function(node) {
				if (lookup_ips.length > 0) {
					lookup_ips += ",";
				}
				lookup_ips += FlowInspector.ipToStr(node.id);
			});
			data["ips"] = lookup_ips;
			this.geoInfoQuery.fetch({data: data});
		}

	},
	updateGeoCache : function() {
		console.log("Updating GeoCache");
		if (this.geoInfoQuery.length > 0) {
			var that = this;
			that.markerList = [];
			this.geoInfoQuery.each(function(node) {
				var myLatlng = new google.maps.LatLng(node.get("latitude"), node.get("longitude"));
				var marker = new google.maps.Marker({
					position: myLatlng,
					title: node.get("ip")
				});
				marker.setMap(this.map);
				that.markerList.push(marker);
			});
		}
		console.log("Updating GeoCache - starting render:");
		console.log(this.markerList);
		this.render();
		console.log("Updating GeoCache - finished render");
	},
});
