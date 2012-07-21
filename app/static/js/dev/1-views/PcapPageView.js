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
	tagName: "ul",
	className : "pcap-status-lines",
	initialize: function(options) {
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
		"click a.submit" : "clickSubmit"
	},
	initialize: function() {
		this.template = _.template($("#pcap-page-template").html());
		this.loaderTemplate = _.template($("#loader-template").html());

		this.pcapAnalysisStatus = false;


		pcapStatusModel = new PCAPLiveLines([]);
		var updateLiveLines = function() {
			this.pcapStatusModel.fetch({add: true});
			setTimeout(updateLiveLines, 2000);
		};
		updateLiveLines();

		if (!this.pcapStatusView) {
			this.pcapStatusView = new PcapStatusLinesView({collection: pcapStatusModel});
		}

		this.statsImages = new PcapStatsView();
	},
	render: function() {
		$(this.el).html(this.template());

		$(".pcap-live-stats", this.el).append(this.pcapStatusView.el);
		$(".stats-images", this.el).append(this.statsImages.el);

		this.statsImages.render();
		this.pcapStatusView.render();

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
		if (this.pcapStatusView) {
		}
		this.statsImages.showPcapAnalysisStatus();
		$("#fileupload").submit();
	}
});
