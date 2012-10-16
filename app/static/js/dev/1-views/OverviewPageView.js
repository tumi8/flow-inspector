var OverviewPageView = PageView.extend({
	events: {
		"click .hostview-value a": "clickHostviewValue",
		"click .bucketview-value a": "clickBucketChartValue",
		"click .donut-chart-value a": "clickDonutChartValue"
	},
	initialize: function() {
		this.template = _.template($("#overview-page-template").html());
		
		this.portsIndex = new IndexQuery(null, { index: "ports" });
		this.nodesIndex = new IndexQuery(null, { index: "nodes" });

		this.nodesDonutModel = new DonutChartModel({ index: "nodes", fetchOnInit: true });
		this.nodesDonutModel.bind("change:value", this.changeDonutChartValue, this);
		this.nodesDonutView = new DonutChartView({ model: this.nodesDonutModel, index: this.nodesIndex });
		
		this.portsDonutModel = new DonutChartModel({ index: "ports", fetchOnInit: true });
		this.portsDonutView = new DonutChartView({ model: this.portsDonutModel, index: this.portsIndex });
		
		this.hostModel = new HostViewModel({fetchOnInit: true});
		this.hostModel.bind("change:value", this.changeHostViewValue, this);
		this.hostView = new HostView({ model: this.hostModel, index: new IndexQuery(null, { index: this.hostModel.get("index") }) });
		
		this.bucketChartModel = new BucketChartModel();
		this.bucketChartModel.bind("change:value", this.changeBucketChartValue, this);
		this.bucketChartView = new BucketChartView({ model: this.bucketChartModel });
	},
	render: function() {
		$(this.el).html(this.template());
		
		$(".hostview-value li[data-value='" + this.hostModel.get("value") + "']", this.el)
			.addClass("active");
		
		$(".bucketview-value li[data-value='" + this.bucketChartModel.get("value") + "']", this.el)
			.addClass("active");
		
		$(".donut-chart-value li[data-value='" + this.nodesDonutModel.get("value") + "']", this.el)
			.addClass("active");

		$(".viz-hostview", this.el).append(this.hostView.el);
		$(".viz-donut-nodes", this.el).append(this.nodesDonutView.el);
		$(".viz-donut-ports", this.el).append(this.portsDonutView.el);
		$(".viz-buckets", this.el).append(this.bucketChartView.el);
	
		this.hostView.render();
		this.nodesDonutView.render();
		this.portsDonutView.render();
		this.bucketChartView.render();
		return this;
	},
	clickHostviewValue: function(e) {
		var target = $(e.target).parent();
		this.hostModel.set({ value: target.data("value") });
	},
	clickBucketChartValue: function(e) {
		var target = $(e.target).parent();
		this.bucketChartModel.set({value: target.data("value")});
	},
	clickDonutChartValue: function(e) {
		var target = $(e.target).parent();
		this.nodesDonutModel.set({ value: target.data("value") });
		this.portsDonutModel.set({ value: target.data("value") });
	},
	changeHostViewValue: function(model, value) {
		$(".hostview-value li", this.el).removeClass("active");
		$(".hostview-value li[data-value='" + value + "']", this.el)
		.addClass("active");
	},
	changeBucketChartValue: function(model, value) {
		$(".bucketview-value li", this.el).removeClass("active");
		$(".bucketview-value li[data-value='" + value + "']", this.el)
			.addClass("active");
	},
	changeDonutChartValue: function(model, value) {
		$(".donut-chart-value li", this.el).removeClass("active");
		$(".donut-chart-value li[data-value='" + value + "']", this.el)
			.addClass("active");
	}
});
