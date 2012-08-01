var PcapStatsView = Backbone.View.extend({
	className: "pcapstatsview",
	initialize: function() {
		this.stats = new PcapStats();
		this.stats.bind("reset", this.render, this);

		this.stats.fetch()
	/*
		// chart formatting
		this.m = [10, 20, 30, 70];
		this.stroke = d3.interpolateRgb("#0064cd", "#c43c35");
    	
		this.index = new IndexQuery(null, { index: this.model.get("index") });
		this.index.bind("reset", this.render, this);
		// fetch at the end because a cached request calls render immediately!
		this.index.fetch();
	*/
	}, render: function() {
		var container = $(this.el).empty(); var w = container.width(); var h = container.height();
		var data = this.stats.models;
		var x = d3.scale.linear().range([0, w]);
		var y = d3.scale.linear().range([0, h]);

		selectedVal = "tcpPkts";

		if (w <= 0) {
			return;
		}

		if (data.length === 0) {
			return;
		}


		//$(this.el).append("<h2>Statistics</h2>");

		data = data.map(function(d) { return {x: parseInt(d.get("second")), y: d.get(selectedVal)}});
		console.log(data)
		//data = [ { x: 0, y: 40 }, { x: 1, y: 49 }, { x: 2, y: 17 }, { x: 3, y: 42 } ];


		var graph = new Rickshaw.Graph( {
			element: $("#packetChart")[0],
			width: w,
			height: h/2,
			series: [ {
				color: 'steelblue',
				data: data
			} ]
		});

/*
		var x_axis = new Rickshaw.Graph.Axis.Time( {graph: graph} );
		var y_axis = new Rickshaw.Graph.Axis.Y( {
			graph: graph,
			orientation: 'left',
			tickFormat: Rickshaw.Fixtures.Number.formatKMBT,
			element: $("#y_axis")[0]
		});
		
*/
		graph.render();
/*


		var n = 10,
			m = 100,
			data0 = d3.layout.stack().offset("wiggle")(this.stream_layers(n, m)),
			data1 = d3.layout.stack().offset("wiggle")(this.stream_layers(n, m)),
			color = d3.interpolateRgb("#aad", "#556");
		
		var width = w,
			height = h,
			mx = m - 1;
			my = d3.max(data0.concat(data1), function (d) {
				return d3.max(d, function(d) {
					return d.y0, d.y;
				});
			});
		var area = d3.svg.area()
			.x(function(d) { return d.x * width / mx; })
			.y0(function(d) { return height - d.y0 *height / my; })
			.y1(function(d) { return height - (d.y - d.y0) * height / my; });
		
		this.svg.selectAll("path")
			.data(data0)
			.enter()
				.append("path")
				.style("fill", function() { return color(Math.random()); })
				.attr("d", area);

		function transition() {
			d3.selectAll("path")
				.data(function() {
					var d = data1;
					data1 = data0;
					return data0 = d;
				})
				.transition()
				.duration(2500)
				.attr("d", area);
		}
		transition();
*/
/*

		var ppsImage = this.svg.append("svg:svg")
			.attr("width", w)
			.attr("height", h/2);

		this.ppsImg = this.svg.append("svg:g");
		this.throughPutImg = this.svg.append("svg:g");

		this.svg.append("svg:image")
			.attr("xlink:href", "/api/pcap/images/pps.svg")
			.attr("width", w)
			.attr("height", h/2);
		this.svg.append("svg:image")
			.attr("xlink:href", "/api/pcap/images/tp.svg")
			.attr("y", h/2)
			.attr("width", w)
			.attr("height", h/2);
//		this.ppsImg.append("label")
//			.text("foobar");
*/

		return this;
	},
	stream_layers : function(n, m, o) {
		if (arguments.length < 3) o = 0;
		function bump(a) {
			var x = 1 / (.1 + Math.random()),
				y = 2 * Math.random() - .5,
				z = 10 / (.1 + Math.random());
			for (var i = 0; i < m; ++i) {
				var w = (i / m - y) * z;
				a[i] += x * Math.exp(-w * w);
			}
		}
		return d3.range(n).map(function() {
			var a = [], i;
			for (i = 0; i < m; ++i) a[i] = o + o * Math.random();
			for (i = 0; i < 5; i++) bump(a);
			return a.map(function(d, i) {return {x: i, y: Math.max(0, d) }});
		})
	}
});
