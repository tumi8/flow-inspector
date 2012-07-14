var PcapPageView = PageView.extend({
	events: {
	},
	initialize: function() {
		alert('foo');
		this.template = _.template($("#pcap-page-template").html());
	},
	render: function() {
		$(this.el).html(this.template());

		return this;
	},
	remove: function() {
		$(this.el).remove();
		return this;
	}
);
