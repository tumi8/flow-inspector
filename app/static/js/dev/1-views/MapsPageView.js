var MapsPageView = PageView.extend({
	events: {
	},
	initialize: function() {
		this.markerList = new Array();
		var myLatlng = new google.maps.LatLng(48.1333, 11.5667);
		var marker = new google.maps.Marker({
			position: myLatlng,
			title: "MÃnchen!"
			});
		this.markerList.push(marker);


		this.template = _.template($("#maps-page-template").html());
		this.nodesIndex = new IndexQuery(null, { index: "nodes" });
		this.nodesIndex.bind("reset", this.getGeoInfo, this);

		this.geoInfoQuery = new GeoInfoQuery()
		this.geoInfoQuery.bind("reset", this.updateGeoCache, this);

		this.nodesIndex.fetch();
	},
	render: function() {
		var that = this;

		$(this.el).html(this.template());

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

		return this;
	},
	getGeoInfo : function() {
		var data = {}
		if (this.nodesIndex.length > 0) {
			var lookup_ips = "";
			this.nodesIndex.each(function(node) {
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
		if (this.geoInfoQuery.length > 0) {
			var that = this;
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
		this.render();
	}
});
