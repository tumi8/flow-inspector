var BucketChartView = Backbone.View.extend({
	className: "bucket-chart",
	events: {},
	initialize: function(options) {
		if(!this.model) {
			this.model = new BucketChartModel();
		}
	    	this.model.bind("change:value", this.changeValue, this);
		this.model.bind("change:interval", this.changeInterval, this);
    		
		this.loaderTemplate = _.template($("#loader-template").html());
    	
		// chart formatting
		this.m = [10, 20, 30, 70];
		this.stroke = d3.interpolateRgb("#0064cd", "#c43c35");

		if (options.fetchEmptyInterval !== undefined) {
			this.model.set({fetchEmptyInterval : options.fetchEmptyInterval})
		}
	
		this.flows = new Flows();
		this.flows.bind("reset", this.render, this);
		// fetch at the end because a cached request calls render immediately!
		this.fetchFlows();
	},
	render: function() {
		var container = $(this.el).empty(),
		num_val = this.model.get("value"),
		w = container.width() - this.m[1] - this.m[3],
		yOffset = 70,
		h = 200 - this.m[0] - this.m[2] + yOffset,
		x = d3.time.scale().range([0, w]),
		y = d3.scale.linear().range([h, yOffset]),
		stroke = this.stroke,
		data = this.flows.models,
		bucket_size = this.flows.bucket_size,
		xAxis = this.getXAxis(h, x),
		yAxis = this.getYAxis(y),
		titleFormat = FlowInspector.getTitleFormat(this.model.get("value"));
    		
		// check if container was removed from DOM	
		if(w <= 0) {
			return;
		}
    		
		// A SVG element.
		this.svg = d3.select(container.get(0))
			.data([data])
			.append("svg:svg")
				.attr("width", w + this.m[1] + this.m[3])
				.attr("height", h + this.m[0] + this.m[2] + yOffset )
    			.append("svg:g")
   				.attr("transform", "translate(" + this.m[3] + "," + this.m[0] + ")");
    	
		if(data.length === 0) {
			container.append(this.loaderTemplate());
			return this;
		}

		this.axisGroup = this.svg.append("svg:g");
		this.barsGroup = this.svg.append("svg:g");
		this.labelGroup = this.svg.append("svg:g");

		// Set the scale domain
		var min_bucket = d3.min(data, function(d) { return d.get(FlowInspector.COL_BUCKET); });
		var max_bucket = d3.max(data, function(d) { return d.get(FlowInspector.COL_BUCKET); });
    		var max_value = d3.max(data, function(d) { return d.get(num_val); });
		x.domain([min_bucket, new Date(max_bucket.getTime() + bucket_size*1000)]);
		y.domain([0, max_value]);
    	   
		this.axisGroup.append("g")
  			.attr("class", "x axis")
  			.attr("transform", "translate(0," + h + ")")
  			.call(xAxis);
    		this.axisGroup.append("g")
  			.attr("class", "y axis")
  			.call(yAxis);
    	
		var bar = this.barsGroup.selectAll("g.bar")
			.data(data);
    		
		var bar_enter =	bar.enter().append("g")
			.attr("class", "bar")
			.attr("transform", function(d) { return "translate(" + x(d.get(FlowInspector.COL_BUCKET)) + ",0)"; })
			.attr("title", titleFormat)
/*
			.on("mouseover", function(d) {
			d3.select(this).selectAll("rect")
				.attr("fill", stroke(d.get(num_val) / max_value));
			})
			.on("mouseout", function(d) {
				d3.select(this).selectAll("rect")
					.attr("fill", "rgba(0,100,205,0.2)");
			})
*/
;

		// the following method aims at getting the appropriate y-offset for 
		// the value of num_val (which can be flows, pakets, or bytes) 
		// for the given protocol (tcp, udp, icmp, others)
		getProtoSpecificY = function(obj, proto, num_val) {
			var val = 1;
			var protoObj = obj.get(proto);
			// the value might not be set in the db. use 0 as default
			if (protoObj) {
				val = protoObj[num_val];
				if (! val > 1) {
					return 1;
				}
			}
			return y(val);
		}

		// tcp bar
		bar_enter.append("rect")
			.attr("class", FlowInspector.COL_PROTO_TCP)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, num_val); })
			.attr("y", function(d) { return getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, num_val); })
			.attr("fill", FlowInspector.tcpColor);

		// udp bar
		bar_enter.append("rect")
			.attr("class", FlowInspector.COL_PROTO_UDP)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, FlowInspector.COL_PROTO_UDP, num_val);  })
			.attr("y", function(d){ return getProtoSpecificY(d, FlowInspector.COL_PROTO_UDP, num_val) - (h - getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, num_val));})
			.attr("fill", FlowInspector.udpColor);

		// icmp bar
		bar_enter.append("rect")
			.attr("class", FlowInspector.COL_PROTO_ICMP)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, FlowInspector.COL_PROTO_ICMP, num_val); })
			.attr("y", function(d) { return getProtoSpecificY(d, FlowInspector.COL_PROTO_ICMP, num_val) - (h - getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, num_val)) - (h - getProtoSpecificY(d, FlowInspector.COL_PROTO_UDP, num_val));})
			.attr("fill", FlowInspector.icmpColor);

		// other bar
		bar_enter.append("rect")
			.attr("class", FlowInspector.COL_PROTO_OTHER)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, FlowInspector.COL_PROTO_OTHER, num_val); })
			.attr("y", function(d) { return getProtoSpecificY(d, FlowInspector.COL_PROTO_OTHER, num_val) - (h - getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, num_val)) - (h - getProtoSpecificY(d, FlowInspector.COL_PROTO_UDP, num_val)) - (h - getProtoSpecificY(d, FlowInspector.COL_PROTO_ICMP, num_val));})
			.attr("fill", FlowInspector.otherColor);

		bar_enter.append("line")
			.attr("x1", 0)
			.attr("x2", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("y1", function(d) { return y(d.get(num_val)); })
			.attr("y2", function(d) { return y(d.get(num_val)); })
			.attr("stroke", function(d) { return stroke(d.get(num_val) / max_value); });
    	
		var legendXOffset = 65;

		this.labelGroup.append("text")
			.attr("x", w-5)
			.attr("y", 5)
			.attr("text-anchor", "end")
			.text("#" + num_val);

		this.labelGroup.append("text")
			.attr("x", w-5)
			.attr("y", 20)
			.attr("text-anchor", "end")
			.text("tcp");

		this.labelGroup.append("rect")
			.attr("width", 20)
			.attr("height", 10)
			.attr("x", w - legendXOffset)
			.attr("y", 10)
			.attr("fill", FlowInspector.tcpColor);

    		this.labelGroup.append("text")
			.attr("x", w-5)
			.attr("y", 35 )
			.attr("text-anchor", "end")
			.text("udp");

		this.labelGroup.append("rect")
			.attr("width", 20)
			.attr("height", 10)
			.attr("x", w - legendXOffset)
			.attr("y", 25)
			.attr("fill", FlowInspector.udpColor);


    		this.labelGroup.append("text")
			.attr("x", w-5)
			.attr("y", 50)
			.attr("text-anchor", "end")
			.text("icmp");

		this.labelGroup.append("rect")
			.attr("width", 20)
			.attr("height", 10)
			.attr("x", w - legendXOffset)
			.attr("y", 40)
			.attr("fill", FlowInspector.icmpColor);


    		this.labelGroup.append("text")
			.attr("x", w-5)
			.attr("y", 65)
			.attr("text-anchor", "end")
			.text("other");

		this.labelGroup.append("rect")
			.attr("width", 20)
			.attr("height", 10)
			.attr("x", w - legendXOffset)
			.attr("y", 55)
			.attr("fill", FlowInspector.otherColor);

	

		$(".bar", this.el).twipsy({ offset: 3 });
    	
		return this;
	},
	changeValue: function(model, value) {
		if(!this.barsGroup) {
			return;
		}
	
		var container = $(this.el),
		w = container.width() - this.m[1] - this.m[3],
		yOffset = 70,
		h = 200 - this.m[0] - this.m[2] + yOffset,
		x = d3.time.scale().range([0, w]),
		y = d3.scale.linear().range([h, yOffset]),
		stroke = this.stroke,
		data = this.flows.models,
		bucket_size = this.flows.bucket_size,
		xAxis = this.getXAxis(h, x),
		yAxis = this.getYAxis(y),
		titleFormat = FlowInspector.getTitleFormat(this.model.get("value"));
		
		// Set the scale domain
		var min_bucket = d3.min(data, function(d) { return d.get(FlowInspector.COL_BUCKET); });
		var max_bucket = d3.max(data, function(d) { return d.get(FlowInspector.COL_BUCKET); });
		var max_value = d3.max(data, function(d) { return d.get(value); });
		x.domain([min_bucket, new Date(max_bucket.getTime() + bucket_size*1000)]);
		y.domain([0, max_value]);
		
		this.axisGroup.selectAll("g").remove();
		this.axisGroup.append("g")
			.attr("class", "x axis")
			.attr("transform", "translate(0," + h + ")")
			.call(xAxis);
		this.axisGroup.append("g")
			.attr("class", "y axis")
			.call(yAxis);
    	
		var bar = this.barsGroup.selectAll("g.bar")
			.data(data)
		.attr("title", titleFormat);


		// the following method aims at getting the appropriate y-offset for 
		// the value of num_val (which can be flows, pakets, or bytes) 
		// for the given protocol (tcp, udp, icmp, others)
		getProtoSpecificY = function(obj, proto, num_val) {
			var val = 1;
			var protoObj = obj.get(proto);
			// the value might not be set in the db. use 0 as default
			if (protoObj) {
				val = protoObj[num_val];
				if (! val > 1) {
					return 1;
				}
			}
			return y(val);
		}

		bar.selectAll("rect.tcp")
			.transition()
			.duration(1000)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, value); })
			.attr("y", function(d) { return getProtoSpecificY(d, FlowInspector.COL_PROTO_TCP, value); })

		bar.selectAll("rect.udp")
			.transition()
			.duration(1000)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, "udp", value);  })
			.attr("y", function(d){ return getProtoSpecificY(d, "udp", value) - (h - getProtoSpecificY(d, "tcp", value));})

		bar.selectAll("rect.icmp")
			.transition()
			.duration(1000)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, "icmp", value); })
			.attr("y", function(d) { return getProtoSpecificY(d, "icmp", value) - (h - getProtoSpecificY(d, "tcp", value)) - (h - getProtoSpecificY(d, "udp", value));})

		bar.selectAll("rect.other")
			.transition()
			.duration(1000)
			.attr("width", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("height", function(d) { return h - getProtoSpecificY(d, "icmp", value); })
			.attr("y", function(d) { return getProtoSpecificY(d, "icmp", value) - (h - getProtoSpecificY(d, "tcp", value)) - (h - getProtoSpecificY(d, "udp", value)) - (h - getProtoSpecificY(d, "icmp", value));})

	
		bar.selectAll("line")
			.transition()
			.duration(1000)
			.attr("x1", 0)
			.attr("x2", x(new Date(min_bucket.getTime() + bucket_size*1000)))
			.attr("y1", function(d) { return y(d.get(value)); })
			.attr("y2", function(d) { return y(d.get(value)); })
			.attr("stroke", function(d) { return stroke(d.get(value) / max_value); });
    	
		this.labelGroup.select("text")
			.text("#" + value);
    		
		$(".bar", this.el).twipsy({ offset: 3 });
	},
	getXAxis: function(h, scaleX) {
		return d3.svg.axis().scale(scaleX)
			.tickSize(-h,2,0)
			.ticks(4)
			.tickSubdivide(10)
			.tickPadding(10)
			.tickFormat(d3.time.format("%Y-%m-%d %H:%M:%S"));
	},
	getYAxis: function(scaleY) {
		var axis = d3.svg.axis().scale(scaleY)
			.orient("left")
			.tickSize(2);
    		
		var value = this.model.get("value");
		if(value === FlowInspector.COL_PKTS) {
			axis.tickFormat(FlowInspector.getTitleFormat(value));
		}
		else if(value === FlowInspector.COL_BYTES) {
			axis.tickFormat(FlowInspector.getTitleFormat(value));
		}
		
		return axis;
	},
	changeInterval: function() {
		$(this.loaderTemplate()).show();
		this.fetchFlows();
	},
	fetchFlows: function() {
		var fetchEmptyInterval = this.model.get("fetchEmptyInterval");
		var interval = this.model.get("interval");
		if (!fetchEmptyInterval && interval.length == 0) {
			return; 
		}

		var data = {
			"resolution": 1000
		}

		if (interval.length > 0) {
			data["start_bucket"] =  Math.floor(interval[0].getTime() / 1000);
			data["end_bucket"] =  Math.floor(interval[1].getTime() / 1000);
		}

		data = FlowInspector.addToFilter(data, this.model, FlowInspector.COL_BUCKET, false);
		if (data == null) {
			return;
		}

		this.flows.fetch({ data: data});
	}
});
