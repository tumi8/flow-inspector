var GraphPageView = PageView.extend({
	events: {
		"click a.reset": "clickLayoutReset",
		"click a.force": "clickLayoutForce",
		"click a.hilbert": "clickLayoutHilbert",
		"change #filterNodeLimit": "changeNodeLimit",
		"blur #filterPorts": "changeFilterPorts",
		"blur #filterIPs": "changeFilterIPs",
		"change #filterPortsType": "changeFilterPortsType",
		"change #filterIPsType": "changeFilterIPsType",
		"change #showOthers": "changeShowOthers"
	},
	initialize: function() {
		this.template = _.template($("#graph-page-template").html());
		this.loaderTemplate = _.template($("#loader-template").html());
		
		var that = this;
		// make function available to unbind later
		this._onResize = function() { that.render(); };
		$(window).bind("resize", this._onResize);
		
		this.nodes = new IndexQuery(null, { index: "nodes" });
		this.flows = new Flows();
		this.timelineModel = new TimelineModel();
		this.graphModel = new GraphModel();

		this.timelineView = new TimelineView({
			model: this.timelineModel
		});
		this.graphView = new GraphView({
			nodes: this.nodes,
			flows: this.flows,
			timeline: this.timelineModel,
			model: this.graphModel
		});
    	
		// bind after initialization of GraphView to get event after the GraphView instance
		this.nodes.bind("reset", this.finishedNodesLoading, this);
		this.flows.bind("reset", this.updateIfLoaded, this);
    	
		this.timelineModel.bind("change:interval", this.changeBucketInterval, this);
		this.graphModel.bind("change:nodeLimit", this.nodeLimitChanged, this);
		this.graphModel.bind("change:filterPorts", this.filterPortsChanged, this);
		this.graphModel.bind("change:filterPortsType", this.filterPortsTypeChanged, this);
		this.graphModel.bind("change:filterIPs", this.filterIPsChanged, this);
		this.graphModel.bind("change:filterIPsType", this.filterIPsTypeChanged, this);
		this.graphModel.bind("change:showOthers", this.showOthersChanged, this);
    	
		// fetch at the end because a cached request calls render immediately!
		this.fetchNodes();
	},
	remove: function() {
		if(this.timelineView) {
			this.timelineView.remove();
		}
		if(this.graphView) {
			this.graphView.remove();
		}
    	
		$(window).unbind("resize", this._onResize);
		$(this.el).remove();
		return this;
	},
	render: function() {
		$(this.el).html(this.template());
    		
		this.loader = $(this.loaderTemplate());
		$(".content", this.el).append(this.loader);
    	
		this.contentScrollApi = $(".content .scroll", this.el)
			.jScrollPane()
			.data("jsp");
		this.asideScrollApi = $("aside", this.el)
			.jScrollPane()
			.data("jsp");
		
		$("#footbar", this.el).append(this.timelineView.el);
		// rewire events because we removed the view from the dom
		this.timelineView.delegateEvents();
		this.timelineView.render();
    	
		$(".canvas", this.el).append(this.graphView.el);
		// rewire events because we removed the view from the dom
		this.graphView.delegateEvents();
		this.graphView.render();
		
		this.updateIfLoaded();
    	
		// form defaults
		$("#filterNodeLimit", this.el).val(this.graphModel.get("nodeLimit"));
		$("#filterPorts", this.el).val(this.graphModel.get("filterPorts"));
		$("#filterPortsType", this.el).val(this.graphModel.get("filterPortsType"));
		$("#showOthers", this.el).attr("checked", this.graphModel.get("showOthers"));
    	
		$("aside .help", this.el).popover({ offset: 24 });

		return this;
	},
	hide: function() {
		this.graphView.stop();

		$(window).unbind("resize", this._onResize);

		return PageView.prototype.hide.call(this);
	},
	show: function() {
		$(window).bind("resize", this._onResize);
		return PageView.prototype.show.call(this);
	},
	finishedNodesLoading: function() {
		// we have a new set of nodes. Reload the appropriate flows
		this.fetchFlows();
	},
	updateIfLoaded: function() {
		if(!$(this.el).html()) {
			this.render();
		}
    	
		if(this.nodes.length <= 0) {
			$(".btn", this.el).addClass("disabled");
			return this;
		}
    	
		$(".btn", this.el).removeClass("disabled");
    	
		this.loader.hide();
    	
		this.contentScrollApi.reinitialise();
		this.contentScrollApi.scrollToPercentX(0.5);
		this.contentScrollApi.scrollToPercentY(0.5);
    	
		return this;
	},
	fetchNodes: function() {
		var data = {};
		var showOthers = this.graphModel.get("showOthers");
		var nodeLimit = this.graphModel.get("nodeLimit");

		data["sort"] = "flows desc";
		if (!showOthers) {
			// we only have to show the nodes and the flows within the 
			// nodeLimit. So we only have to fetch the nodeLimit nodes with the most
			// flows attached to them.
			data["limit"] = nodeLimit;
		}
		this.nodes.fetch({data : data});
	},
	fetchFlows: function() {
		var interval = this.timelineModel.get("interval");
		var bucket_size = this.timelineModel.get("bucket_size");
		var filter_ports = $.trim(this.graphModel.get("filterPorts"));
		var filter_ports_type = this.graphModel.get("filterPortsType");
		var showOthers = this.graphModel.get("showOthers");
    	
		var data = { 
			"fields": "srcIP,dstIP",
			"start_bucket": Math.floor(interval[0].getTime() / 1000),
			"end_bucket": Math.floor(interval[1].getTime() / 1000),
			"bucket_size": bucket_size,
			"biflow": 1
		};
    	
		var ports = filter_ports.split("\n");
		filter_ports = "";
		for(var i = 0; i < ports.length; i++) {
			var p = parseInt(ports[i]);
    			// test for NaN
    			if(p === p) {
    				if(filter_ports.length > 0) {
    					filter_ports += ",";
    				}
    				filter_ports += p;
    			}
		}
		
		if(filter_ports) {
			if(filter_ports_type === "exclusive") {
				data["exclude_ports"] = filter_ports;
			} else {
				data["include_ports"] = filter_ports;
			}
		}
		
		if (!showOthers) {
			// we only need to take flows to the top nodes within the nodeLimit
			// into account. So if we have the nodes, we can limit our flow extraction 
			// to them ...
			if (this.nodes.length > 0) {
				var filter_ips = "";
				this.nodes.each(function(node) {
					if (filter_ips.length > 0) {
						filter_ips += ",";
					}
					filter_ips += node.id;
				});
				data["include_ips"] = filter_ips;
			}
		}

		this.flows.fetch({ data: data });
	},
	changeBucketInterval: function(model, interval) {
		this.loader.show();
		this.fetchFlows();
	},
	clickLayoutReset: function() {
		if(this.nodes.length <= 0 || this.flows.length <= 0) {
			return;
		}
		this.graphView.forceLayout(true);
	},
	clickLayoutForce: function() {
		if(this.nodes.length <= 0 || this.flows.length <= 0) {
			return;
		}
		this.graphView.forceLayout();
	},
	clickLayoutHilbert: function() {
		if(this.nodes.length <= 0 || this.flows.length <= 0) {
			return;
		}
		this.graphView.hilbertLayout();
	},
	changeNodeLimit: function() {
		this.graphModel.set({
			nodeLimit: Number($("#filterNodeLimit", this.el).val())
		});
	},
	nodeLimitChanged: function(model, value) {
		$("#filterNodeLimit", this.el).val(value);
		this.loader.show();
		this.fetchNodes();
	},
	showOthersChanged: function(model, value) {
		$("#showOthers", this.el).attr("checked", value);
		this.loader.show();
		this.fetchNodes();
	},
	changeFilterPorts: function() {
		this.graphModel.set({
			filterPorts: $("#filterPorts", this.el).val()
		});
	},
	changeFilterIPs: function() {
		alert("changeFilterIPs");
	},
	filterPortsChanged: function(model, value) {
		$("#filterPorts", this.el).val(value);
		this.loader.show();
		this.fetchFlows();
	},
	filterIPsChanged: function(model, value) {
		$("#filterIPs", this.el).val(value);
		// TODO: We have to check the nodes, too ...
		this.loader.show();
		this.fetchFlows();
	},
	changeFilterPortsType: function() {
		this.graphModel.set({
			filterPortsType: $("#filterPortsType", this.el).val()
		});
	},
	changeFilterIPsType: function() {
		this.graphModel.set({
			filterIPsType: $("#filterIPsType", this.el).val()
		});
	},
	filterPortsTypeChanged: function(model, value) {
		$("#filterPortsType", this.el).val(value);
		this.loader.show();
		this.fetchFlows();
	},
	filterIPsTypeChanged: function(model, value) {
		$("#filterIPsType", this.el).val(value);
		this.loader.show();
		// TODO: we also have to check the nodes ...
		this.fetchFlows();
	},
	changeShowOthers: function() {
		var checkbox = $("#showOthers", this.el);
		var val = false;
		if (checkbox.attr('checked')) {
			val = true;
		}
		this.graphModel.set({
			showOthers: val
		});
	}
});
