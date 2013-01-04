var MapsPageView = PageView.extend({
	events: {
		"click .timeline-value a": "clickTimelineValue",
		"click .hostview-value a": "clickHostviewValue",
	},
	initialize: function() {

		this.timelineModel = new TimelineModel();
		this.timelineView = new TimelineView({
			model: this.timelineModel
		});
		this.timelineModel.bind("change:interval", this.changeBucketInterval, this);
		this.timelineModel.bind("change:value", this.changeTimelineValue, this);
		this.timelineModel.bind("change:bucket_size", this.changeBucketSize, this);


		this.template = _.template($("#maps-page-template").html());
		this.nodesIndex = new DynamicIndexQuery(null, {index: "nodes"});
		this.nodesIndex.bind("reset", this.getGeoInfo, this);

		this.mapViewModel =  new MapsViewModel();
		this.mapView = new MapsView({ model: this.mapViewModel, index: new DynamicIndexQuery(null, { index: "nodes" }), fetchEmptyInterval: false});

		this.hostModel = new HostViewModel();
		this.hostModel.bind("change:value", this.changeHostViewValue, this);
		this.hostView = new HostView({ model: this.hostModel, index: new DynamicIndexQuery(null, { index: this.hostModel.get("index") }), fetchEmptyInterval: false});
	},
	render: function() {
		var that = this;

		$(this.el).html(this.template());

		$(".timeline-value li[data-value='" + this.timelineModel.get("value") + "']", this.el)
			.addClass("active");
		$(".hostview-value li[data-value='" + this.hostModel.get("value") + "']", this.el)
			.addClass("active");
	
		$(".viz-mapview", this.el).append(this.mapView.el);
		$("#footbar", this.el).append(this.timelineView.el);

		this.hostView.render();
		return this;
	},
	clickTimelineValue: function(e) {
		var target = $(e.target).parent();
		this.timelineModel.set({ value: target.data("value") });
	},
	changeTimelineValue: function(model, value) {
		$(".timeline-value li", this.el).removeClass("active");
		$(".timeline-value li[data-value='" + value + "']", this.el)
			.addClass("active");
	},
	changeBucketInterval: function(model, interval) {
		// change models (if any)
		this.mapViewModel.set({interval: interval});
	},
	changeBucketSize: function(model, bucket_size) {
		// change models if any
		this.mapViewModel.set({bucket_size: bucket_size});
	},
	clickTimelineValue: function(e) {
		var target = $(e.target).parent();
		this.timelineModel.set({ value: target.data("value") });
	},
	changeHostViewValue: function(model, value) {
		$(".hostview-value li", this.el).removeClass("active");
		$(".hostview-value li[data-value='" + value + "']", this.el)
			.addClass("active");
	},
	clickHostviewValue: function(e) {
		var target = $(e.target).parent();
		this.hostModel.set({ value: target.data("value") });
		this.mapsModel.set({ value: target.data("value") });
	},

});
