var QueryPageView = PageView.extend({
	events: {
		"click a.submit": "clickSubmit"	
	},
	initialize: function() {
		this.template = _.template($("#query-page-template").html());

		var that = this;
		this._onResize = function() { that.render(); };
		$(window).bind("resize", this._onResize);

		this.model = new QueryModel();

		this.nodes = new IndexQuery(null, {index: "nodes" });
		this.flows = new Flows();

		this.flows.bind("reset", this.resetFlows, this);

		this.timelineModel = new TimelineModel();
		this.timelineView  = new TimelineView({
			model: this.timelineModel
		});

		this.resultsView = new QueryResultsView({
			nodes: this.nodes,
			flows: this.flows
		});

		this.flows.fetch({data: { "resolution": 1000} });

		this.timelineModel.bind("change:interval", this.changeBucketInterval, this);
	},
	resetFlows : function() {
		this.max_bucket = d3.max(this.flows.models, function(d) { return d.get("bucket"); });
		this.min_bucket = d3.min(this.flows.models, function(d) { return d.get("bucket"); });
		this.flowMinTime = this.min_bucket;
		this.flowMaxTime = new Date(this.max_bucket.getTime() + this.flows.bucket_size * 1000);

		this.render();
		return this;
	},
	render: function() {
		$(this.el).html(this.template());

		var flowData = this.flows.models;

		// set the labels for our time input fields
		// with the first and last flow times in our 
		// data set. 
		//TODO: should we include this into our data model?
		$(".query-min-flowtime", this.el).html("<label>First Flow:<br/> " + this.flowMinTime + "</label>");
		$(".query-max-flowtime", this.el).html("<label>Last Flow:<br/> " + this.flowMaxTime + "</label>");

		// add timeline
		$("#footbar", this.el).append(this.timelineView.el);
		this.timelineView.delegateEvents();
		this.timelineView.render();


		// add the results shower
		$(".query-results", this.el).append(this.resultsView);
		this.resultsView.render();

		return this;
	},
	show: function() {
		$(window).bind("resize", this._onResize);
		return PageView.prototype.show.call(this);
	},
	hide: function() {
		$(window).unbind("resize", this._onResize);
		return PageView.prototype.hide.call(this);
	},
	remove: function() {
		if (this.timelineView) {
			this.timelineView.remove();
		}
		if (this.resultsView) {
			this.resultsView.remove();
		}
		$(this.el).remove();
		return this;
	},
	clickSubmit: function() {
		var qmin = $("#qmin").val();
		var startDate = new Date(qmin);
		if (startDate == "Invalid Date") {
			alert("Invalid start date!");
			return 
		} 

		var qmax = $("#qmax").val();
		var endDate = new Date(qmax);
		if (endDate == "Invalid Date") {
			alert("Invalid end date!");
			return;
		} 

		//var ipAddress = FlowInspector.strToIP($("#ipAddress").val());
		var ipField = $("#ipAddress").val();
		if (ipField == "") {
			alert("You need to define an IP address!");
			return;
		}
		var ipAddress = FlowInspector.strToIp(ipField);
		if (!ipAddress) {
			alert("IP address is invalid!");
			return;
		}

		var maxLines = $("#maxLines").val();
		if (maxLines == "" || isNan(maxLines)) {
			alert("You need to define a maximum number of lines to show!");
			return ;
		}
	},
	changeBucketInterval : function() {
		var interval = this.timelineModel.get("interval");
		var bucket_size = this.timelineModel.get("bucket_size");
		$("#qmin").val(interval[0]);
		$("#qmax").val(new Date(interval[1].getTime() + bucket_size * 1000));
	}
});
