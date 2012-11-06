var EdgeBundlePageView = PageView.extend({
	events: {
		"change #layoutTension": "changeTension",
		"change #filterGroupBytes": "changeGroupBytes",
		"change #filterNodeLimit": "changeNodeLimit",
		"change #hoverDirection": "changeHoverDirection",

		"click a.apply-filter": "clickApplyFilter",
		"blur #filterProtocols": "changeFilterProtocols",
		"blur #filterPorts": "changeFilterPorts",
		"blur #filterIPs": "changeFilterIPs",
		"change #filterPortsType": "changeFilterPortsType",
		"change #filterProtocolsType": "changeFilterProtocolsType",
		"change #filterIPsType": "changeFilterIPsType",

	},
	initialize: function() {
		this.template = _.template($("#edge-bundle-page-template").html());
		this.loaderTemplate = _.template($("#loader-template").html());
		
		var that = this;
		// make function available to unbind later
		this._onResize = function() { that.render(); };
    		$(window).bind("resize", this._onResize);
    	
		this.nodes = new IndexQuery(null, { index: "nodes" });
		this.flows = new Flows();
		this.timelineModel = new TimelineModel();
		this.edgeBundleModel = new EdgeBundleModel();

		this.timelineView = new TimelineView({
			model: this.timelineModel
		});
		this.edgeBundleView = new EdgeBundleView({
			nodes: this.nodes,
			flows: this.flows,
			timeline: this.timelineModel,
			model: this.edgeBundleModel
		});

		// bind after initialization of GraphView to get event after the GraphView instance
		this.nodes.bind("reset", this.updateIfLoaded, this);
		this.flows.bind("reset", this.updateIfLoaded, this);
    	
		this.timelineModel.bind("change:interval", this.changeBucketInterval, this);
		this.edgeBundleModel.bind("change:tension", this.tensionChanged, this);
		this.edgeBundleModel.bind("change:groupBytes", this.groupBytesChanged, this);
		this.edgeBundleModel.bind("change:nodeLimit", this.nodeLimitChanged, this);
		this.edgeBundleModel.bind("change:hoverDirection", this.hoverDirectionChanged, this);
    	
		// fetch at the end because a cached request calls render immediately!
		this.nodes.fetch();
	},
	remove: function() {
    	
		if(this.timelineView) {
			this.timelineView.remove();
		}
		if(this.edgeBundleView) {
			this.edgeBundleView.remove();
		}
    	
		$(window).unbind("resize", this._onResize);
		$(this.el).remove();
		return this;
	},
	render: function() {
		$(this.el).html(this.template());
		
		this.loader = $(this.loaderTemplate());
		$(".content", this.el).append(this.loader);
		
		this.asideScrollApi = $("aside", this.el)
			.jScrollPane()
			.data("jsp");
    			
		$("#footbar", this.el).append(this.timelineView.el);
		// rewire events because we removed the view from the dom
		this.timelineView.delegateEvents();
		this.timelineView.render();
		
		$(".canvas", this.el).append(this.edgeBundleView.el);
		// rewire events because we removed the view from the dom
		this.edgeBundleView.delegateEvents();
		this.edgeBundleView.render();
    		
		this.updateIfLoaded();
    	
		// form defaults
		$("#layoutTension", this.el).val(this.edgeBundleModel.get("tension"));
		$("#filterGroupBytes", this.el).val(this.edgeBundleModel.get("groupBytes"));
		$("#filterNodeLimit", this.el).val(this.edgeBundleModel.get("nodeLimit"));
		$("#hoverDirection", this.el).val(this.edgeBundleModel.get("hoverDirection"));

    		$("#filterPorts", this.el).val(this.edgeBundleModel.get("filterPorts"));
		$("#filterPortsType", this.el).val(this.edgeBundleModel.get("filterPortsType"));
    		$("#filterIPs", this.el).val(this.edgeBundleModel.get("filterIPs"));
		$("#filterIPsType", this.el).val(this.edgeBundleModel.get("filterIPsType"));
    		$("#filterProtocols", this.el).val(this.edgeBundleModel.get("filterProtocols"));
		$("#filterProtocolsType", this.el).val(this.edgeBundleModel.get("filterProtocolsType"));

	
		$("aside .help", this.el).popover({ offset: 24 });
		
		return this;
	},
	hide: function() {
		$(window).unbind("resize", this._onResize);
		return PageView.prototype.hide.call(this);
	},
	show: function() {
		$(window).bind("resize", this._onResize);
		return PageView.prototype.show.call(this);
	},
	updateIfLoaded: function() {
		if(!$(this.el).html()) {
			this.render();
		}
    	
		this.loader.hide();
    	
		return this;
	},
	fetchFlows: function() {
		var interval = this.timelineModel.get("interval");
		var bucket_size = this.timelineModel.get("bucket_size");

    		var filter_ports = this.edgeBundleModel.get("filterPorts");
		var filter_ports_type = this.edgeBundleModel.get("filterPortsType");
		var filter_ips = this.edgeBundleModel.get("filterIPs");
		var filter_ips_type = this.edgeBundleModel.get("filterIPsType");
		var filter_protocols = this.edgeBundleModel.get("filterProtocols");
		var filter_protocols_type = this.edgeBundleModel.get("filterProtocolsType");
		
		var data = { 
			"aggregate": FlowInspector.COL_SRC_IP + "," + FlowInspector.COL_DST_IP + "," + FlowInspector.COL_BUCKET,
			"start_bucket": Math.floor(interval[0].getTime() / 1000),
			"end_bucket": Math.floor(interval[1].getTime() / 1000),
			"bucket_size": bucket_size,
		};
    
		// apply filter for ports
		var ports = filter_ports.split("\n");
		filter_ports = "";
		for(var i = 0; i < ports.length; i++) {
			var p = parseInt(ports[i]);
    			// test for nan
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

		// apply filter for ips
		var ips = filter_ips.split("\n");
		filter_ips = "";
		for(var i = 0; i < ips.length; i++) {
			var p = FlowInspector.strToIp(ips[i]);
    			if(p != null) {
    				if(filter_ips.length > 0) {
    					filter_ips += ",";
    				}
    				filter_ips += p;
    			}
		}
		if(filter_ips) {
			if(filter_ips_type === "exclusive") {
				data["exclude_ips"] = filter_ips;
			} else {
				data["include_ips"] = filter_ips;
			}
		}

		// apply filter for ips
		var protocols = filter_protocols.split("\n");
		filter_protocols = "";
		for(var i = 0; i < protocols.length; i++) {
			if(filter_protocols.length > 0) {
				filter_protocols += ",";
			}
    			filter_protocols += protocols[i];
		}
		if(filter_protocols) {
			if(filter_protocols_type === "exclusive") {
				data["exclude_protos"] = filter_protocols;
			} else {
				data["include_protos"] = filter_protocols;
			}
		}

	
		this.flows.fetch({ data: data });
	},
	changeBucketInterval: function(model, interval) {
		this.loader.show();
		this.fetchFlows();
	},
	changeTension: function() {
		this.edgeBundleModel.set({
			tension: Number($("#layoutTension", this.el).val())
		});
	},
	tensionChanged: function(model, tension) {
		$("#layoutTension", this.el).val(tension);
	},
	changeGroupBytes: function() {
		this.edgeBundleModel.set({
			groupBytes: Number($("#filterGroupBytes", this.el).val())
		});
	},
	groupBytesChanged: function(model, value) {
		$("#filterGroupBytes", this.el).val(value);
	},
	changeNodeLimit: function() {
		this.edgeBundleModel.set({
			nodeLimit: Number($("#filterNodeLimit", this.el).val())
		});
	},
	nodeLimitChanged: function(model, value) {
		$("#filterNodeLimit", this.el).val(value);
	},
	changeHoverDirection: function() {
		this.edgeBundleModel.set({
			hoverDirection: $("#hoverDirection", this.el).val()
		});
	},
	hoverDirectionChanged: function(model, value) {
		$("#hoverDirection", this.el).val(value);
	},
	changeFilterProtocols : function(model, value) {
		this.edgeBundleModel.set({
			filterProtocols: $("#filterProtocols", this.el).val()
		});
	},
	changeFilterPorts : function(model, value) {
		this.edgeBundleModel.set({
			filterPorts: $("#filterPorts", this.el).val()
		});
	},
	changeFilterIPs : function(model, value) {
		this.edgeBundleModel.set({
			filterIPs: $("#filterIPs", this.el).val()
		});
	},
	changeFilterPortsType : function(model, value) {
		this.edgeBundleModel.set({
			filterPortsType: $("#filterPortsType", this.el).val()
		});
	},
	changeFilterProtocolsType : function(model, value) {
		this.edgeBundleModel.set({
			filterProtocolsType: $("#filterProtocolsType", this.el).val()
		});
	},
	changeFilterIPsType : function(model, value) {
		this.edgeBundleModel.set({
			filterIPsType: $("#filterIPsType", this.el).val()
		});
	},
	clickApplyFilter : function() {
		this.loader.show();
		this.fetchFlows();
	},


});
