var PcapPageView = PageView.extend({
	events: {
		"click a.submit" : "clickSubmit"
	},
	initialize: function() {
		this.template = _.template($("#pcap-page-template").html());

		this.statsImages = new PcapStatsView();
	},
	render: function() {
		$(this.el).html(this.template());

		$(".stats-images", this.el).append(this.statsImages.el);

		this.statsImages.render();

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
		$("#fileupload").submit();
	}
});
