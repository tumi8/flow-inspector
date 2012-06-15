var OverviewPageView = PageView.extend({
    events: {
    	"click .hostview-value a": "clickHostviewValue",
	"click .bucketview-value a": "clickBucketChartValue"
    },
    initialize: function() {
    	this.template = _.template($("#overview-page-template").html());
    	
	
    	this.overviewModel = new OverviewModel();
    	this.overviewModel.bind("change:value", this.changeOverviewValue, this);
    	this.overviewView = new OverviewView({ model: this.overviewModel });


        this.bucketChartModel = new BucketChartModel();
        this.bucketChartModel.bind("change:value", this.changeBucketChartValue, this);
        this.bucketChartView = new BucketChartView({ model: this.bucketChartModel });
    },
    render: function() {
    	$(this.el).html(this.template());
    	
    	$(".hostview-value li[data-value='" + this.overviewModel.get("value") + "']", this.el)
    		.addClass("active");

    	$(".bucketview-value li[data-value='" + this.bucketChartModel.get("value") + "']", this.el)
    		.addClass("active");


	$(".viz-hostview", this.el).append(this.overviewView.el);	
	$(".viz-buckets", this.el).append(this.bucketChartView.el);
	
	this.overviewView.render();
	this.bucketChartView.render();
	return this;
    },
	clickHostviewValue: function(e) {
		var target = $(e.target).parent();
		this.overviewModel.set({ value: target.data("value") });
	},
	clickBucketChartValue: function(e) {
		var target = $(e.target).parent();
		this.bucketChartModel.set({value: target.data("value")});
	},
	changeOverviewValue: function(model, value) {
		$(".hostview-value li", this.el).removeClass("active");
		$(".hostview-value li[data-value='" + value + "']", this.el)
		.addClass("active");
	},
	changeBucketChartValue: function(model, value) {
		$(".bucketview-value li", this.el).removeClass("active");
		$(".bucketview-value li[data-value='" + value + "']", this.el)
			.addClass("active");
	}
});
