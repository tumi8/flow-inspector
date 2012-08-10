var OverviewView = Backbone.View.extend({
	className: "overview",
	events: {},
	initialize: function() {
		if(!this.model) {
			this.model = new OverviewModel();
		}
    	
		this.model.bind("change:value", this.render, this);
		this.loaderTemplate = _.template($("#loader-template").html());
    	
		// chart formatting
		this.m = [10, 20, 30, 70];
		this.stroke = d3.interpolateRgb("#0064cd", "#c43c35");
    	
		this.index = new IndexQuery(null, { index: this.model.get("index") });
		this.index.bind("reset", this.render, this);
		// fetch at the end because a cached request calls render immediately!
		this.index.fetch();
	},
	render: function() {
		var
			container = $(this.el).empty()
			w = container.width(),
			h = 200,
			data = this.index.models,
			num_val = this.model.get("value"),
			stroke = this.stroke
			;
		if (w <= 0) {
			return ;
		}

		if(data.length === 0) {
			container.append(this.loaderTemplate());
			return this;
		}
		// sort data
		data.sort(function(a,b) {
			return b.get(num_val, 0) - a.get(num_val, 0);
		});

		// take the first 10 elements
		var num_elements = 15
		var data = data.slice(0, num_elements);


		this.svg = d3.select(container.get(0))
			.append("svg:svg")
			.attr("width", w + this.m[1] + this.m[3])
			.attr("height", h + this.m[0] + this.m[2]);

		this.labelGroup = this.svg.append("svg:g");
		this.barGroup = this.svg.append("svg:g");

		var y = d3.scale.linear().range([0, h]);
		var min_value = d3.min(data, function(d) { return d.get(num_val); });
		var max_value = d3.max(data, function(d) { return d.get(num_val); });
		y.domain([0, data.length]);


		// add text to bars
		this.labelGroup.selectAll("text.yAxis")
			.data(data)
			.enter()
				.append("svg:text")
					.attr("x", 0)
					.attr("y", function(d, idx) { return y(idx); })
					.attr("dx", -barWidth/2)
					.attr("dy", "1.2em")
					.attr("text-anchor", "left")
  					.attr("style", "font-size: 12; font-family: Helvetica, sans-serif")
					.text(function(d) { return FlowInspector.ipToStr(d.id); })
					.attr("class", "yAxis");


		var offset = d3.select('text.yAxis').node().getComputedTextLength() + 15;
		var x = d3.scale.linear().range([0, w - offset]);
		x.domain([0, max_value]);


		// draw the bars
		var barWidth = h / data.length - 2;
		var bar = this.barGroup.selectAll("rect")
			.data(data);

		var bar_enter = bar.enter().append("g")
			.attr("class", "bar")
			.attr("title", function(d) { return d.get(num_val) + " " + num_val; })
			.on("mouseover", function(d) {
				d3.select(this).selectAll("rect")
					.attr("fill", stroke(d.get(num_val) / max_value));
			})
			.on("mouseout", function(d) {
				d3.select(this).selectAll("rect")
					.attr("fill", "rgba(0,100,205,0.2)");
			});
			

		bar_enter.append("rect")
				.attr("x", offset)
				.attr("y", function(d, idx) { return y(idx); })
				.attr("width", function(d) { return x(d.get(num_val)); })
				.attr("height", barWidth)
				.attr("fill", "rgba(0,100,205,0.2)");

		bar_enter.append("line")
			.attr("x1", function(d) { return x(d.get(num_val)) + offset; })
			.attr("x2", function (d) {return x(d.get(num_val)) + offset; })
			.attr("y1", function(d, idx) { return y(idx); })
			.attr("y2", function(d, idx) { return y(idx) + barWidth; })
			.attr("stroke", function(d) { return stroke(d.get(num_val) / max_value); });
		

		$(".bar", this.el).twipsy({offset: 3});

		return this;
	},
	changeIndex: function(model, value) {
		if (w <= 0) {
			return ;
		}

		this.index.index = value;
		this.index.fetch();
		return this;
	}
});
