var PcapPageView = PageView.extend({
	events: {
	},
	initialize: function() {
		this.template = _.template($("#pcap-page-template").html());
	},
	render: function() {
		$(this.el).html(this.template());

		var container = $(this.el).empty();
		svg = d3.select(container.get(0))
			.append("svg:svg")
			.attr("width", 200)
			.attr("height", 200);
		//var node  
		$(".stats-pictures", this.el).append(svg);

		return this;
	},
	remove: function() {
		$(this.el).remove();
		return this;
	}
});
