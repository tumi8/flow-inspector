var PcapStatusLineView = Backbone.View.extend({
	tagName: "li",
	className: "pcap-status-line",
	render: function() {
		$(this.el).html(this.model.get("line"));
		return this;
	}
});

// A rendering of a collection of status lines.
var PcapStatusLinesView = Backbone.View.extend({
	className : "pcap-status-lines",
	tagName: "ul",
	initialize: function(options) {
		$(this.el).prepend("<h2>Live PCAP Analysis statistics</h2>");
		this.collection.bind("add", function(model) {
							var pcapStatusLineView = new PcapStatusLineView({model: model});
							$(this.el).append(pcapStatusLineView.render().el);
					}, this);
	},
	render: function() {
		return this;
	}
});


var PcapPageView = PageView.extend({
	events: {
		"click a.submit" : "clickSubmit",
		"click .pcap-stat-value a": "clickFlowGroupValue"
	},
	initialize: function() {
		this.template = _.template($("#pcap-page-template").html());
		this.loaderTemplate = _.template($("#loader-template").html());

		this.statsImages = new PcapStatsView();
		this.statusModel = null;
		this.createLiveStatusView();

		this.flowTableModel = new FlowTableModel();
		this.flowTableModel.bind("change:value", this.changeFlowGroupValue, this);

		this.pcapFlows = new PcapFlows();
		this.pcapFlows.bind("reset", this.render, this);

		this.flowTableView = new FlowTableView({model:this.flowTableModel, flows: this.pcapFlows});

	},
	render: function() {
		$(this.el).html(this.template());

		this.loader = $(this.loaderTemplate());

		
		if (this.flowTableModel &&  this.flowTableModel.get("value") != "") {
			$(".pcap-stat-value li[data-value='" + this.flowTableModel.get("value") + "']", this.el)
			.addClass("active");
		}

		// We show either the pcapanalysis status or the final images
		if (this.pcapStatusView) {
			if (this.statusModel && this.statusModel.isRunning) {
				$(".pcap-live-stats", this.el).append(this.pcapStatusView.el);
				this.pcapStatusView.render();
			} else {
				$(".stats-images", this.el).append(this.statsImages.el);
				this.statsImages.render();
			}
		} else {
			$(".stats-images", this.el).append(this.statsImages.el);
			this.statsImages.render();
		}
		if (this.flowTableView) {
			$(".pcap-stats", this.el).append(this.flowTableView.el);
		}

		return this;
	},
	remove: function() {
		$(this.el).remove();
		return this;
	},
	clickSubmit : function() {
		// check if filename has been given
		if ($("#data").val() == "") {
			alert("You need to select a file!");
			return;
		}
		//this.createLiveStatusView();
		$("#fileupload").submit();
	},
	clickFlowGroupValue: function(e) {
		var target = $(e.target).parent();
		this.pcapFlows.value = target.data("value");
		this.flowTableModel.set({value: target.data("value")});
	},
	changeFlowGroupValue: function(model, value) {
		$(".pcap-stat-value li", this.el).removeClass("active");
		$(".pcap-stat-value li[data-value='" + value + "']", this.el)
			.addClass("active");
		this.fetchFlows(value);
	},
	fetchFlows: function(value) {
		this.pcapFlows.dataTypes = [ "firstTs", "lastTs" ,"src","dst","sPort","dPort","proto","pkts","bytes","maxDiff","medianDiff","avgThroughput" ]
		var data = {
			"fields": "firstTs,lastTs,src,dst,sPort,dPort,proto,pkts,bytes,maxDiff,medianDiff,avgThroughput",
			"limit": 100
		};
		this.pcapFlows.fetch({data: data});
	},
	createLiveStatusView : function() {
		pcapStatusModel = new PCAPLiveLines([]);
		this.statusModel = pcapStatusModel;
		if (!this.pcapStatusView) {
			this.pcapStatusView = new PcapStatusLinesView({collection: pcapStatusModel});
		}

		var that = this;
		var updateLiveLines = function() {
			that.statusModel.fetch({add: true});
			if (!that.statusModel.isRunning) {
				// if the pcap file is fully processed, we should
				// move remove the live status view;
				delete that.statusModel;
				delete that.pcapStatusView;
				that.statusModel = null;
				that.pcapStatusView = null;
				that.render();
				return ;
			}
			that.render();
			setTimeout(updateLiveLines, 100);
		};
		updateLiveLines();
	}
});
