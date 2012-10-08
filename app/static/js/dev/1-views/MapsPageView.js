var MapsPageView = PageView.extend({
	events: {
	},
	initialize: function() {
		this.template = _.template($("#maps-page-template").html());
	},
	render: function() {
		$(this.el).html(this.template());

		var myLatlng = new google.maps.LatLng(48.1333, 11.5667);

		var mapOptions = {
			center: new google.maps.LatLng(48.1333, 11.5667),
			zoom : 2,
			mapTypeId : google.maps.MapTypeId.ROADMAP
		};
		var map = new google.maps.Map(
			document.getElementById("map_canvas"),
			mapOptions);

		var marker = new google.maps.Marker({
			position: myLatlng,
			title: "MÃnchen!"
			});

		marker.setMap(map);

		return this;
	}
});
