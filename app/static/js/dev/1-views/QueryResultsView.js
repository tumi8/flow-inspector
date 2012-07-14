var QueryResultsView =  Backbone.View.extend({
	className: "query-response-view",
	initialize: function(options) {

		this.nodes = options.nodes;
		this.flows = options.flows;

		this.flows.bind("reset", this.updateFlows, this);

		return this;
	},
	render: function() {
		var container = $(this.el).empty();
	},
	remove: function() {
		$(this.el).remove();
		return this;
	},
	updateFlows: function() {
		return this;
	}
});
