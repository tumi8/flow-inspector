var MainRouter = Backbone.Router.extend({
	routes: {
   		"": "overviewDashboard",
   		"dashboard": "pageDashboard",
		"flow-details": "flowDetailsPage",
		"query-page": "queryPage",
   		"graph": "pageGraph",
   		"hierarchical-edge-bundle": "pageEdgeBundle",
   		"hive-plot": "pageHivePlot",
		"maps": "pageMaps",
		"ip-documentation": "pageIPDocumentation",
	},
	initialize: function(options) {
		this.model = options.model;
	},
	overviewDashboard: function() {
		this.model.set({page: "overview"});
	},
	flowDetailsPage: function() {
		this.model.set({page: "flow-details"});
	},
	queryPage: function() {
		this.model.set({page: "query-page"});
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
	},
	pageMaps: function() {
		this.model.set({page: "maps"});
	},
	pageIPDocumentation: function() {
		this.model.set({page: "ip-documentation"});
	},
});

