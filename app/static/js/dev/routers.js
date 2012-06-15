var MainRouter = Backbone.Router.extend({
	routes: {
   		"": "overviewDashboard",
   		"dashboard": "pageDashboard",
   		"graph": "pageGraph",
   		"hierarchical-edge-bundle": "pageEdgeBundle",
   		"hive-plot": "pageHivePlot"
	},
	initialize: function(options) {
		this.model = options.model;
	},
	overviewDashboard: function() {
		this.model.set({page: "overview"});
	},
	pageDashboard: function() {
		this.model.set({page: "dashboard"});
	},
	pageGraph: function() {
		this.model.set({page: "graph"});
	},
	pageEdgeBundle: function() {
		this.model.set({page: "edge-bundle"});
	},
	pageHivePlot: function() {
		this.model.set({page: "hive-plot"});
	}
});

