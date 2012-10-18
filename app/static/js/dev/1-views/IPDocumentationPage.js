var UndocumentedTableView = Backbone.View.extend({
	events: {},
	className: "undocumentedTableView",
	initialize: function(options) {
		if (options && options.model) {
			this.model = options.model;
			this.undocumentedModel = options.model;
		} else {
			this.model = new UndocumentedTableModel();
			this.undocumentedModel = new UndocumentedIPs();
		}

		this.model.bind("change:value", this.render, this);

		
		this.undocumentedModel.bind("reset", this.render, this);

		this.loaderTemplate = _.template($("#loader-template").html());
	},
	render: function() {
		var container = $(this.el).empty()
			w = container.width(),
			h = container.height(),
			data = this.undocumentedModel.models
			;
		if (w <= 0) {
			return
		}
		var table = d3.select(container.get(0))
			.data(data)
			.append("table");
		var thead = table.append("thead");
		var tbody = table.append("tbody");

		thead.append("tr")
			.selectAll("th")
			.data(this.flows.dataTypes)
			.enter()
			.append("th")
				.text(function(type) { return type; });

		var rows = tbody.selectAll("tr")
				.data(data)
				.enter()
				.append("tr");
		var that = this;
		var cells = rows.selectAll("td")
				.data(function(row) {
					return that.flows.dataTypes.map(function(type) {
						var v = row.attributes[type];
						if (type == "firstTs" || type == "lastTs") {
							 var tmp = new Date(v);
							 v = tmp.toString('HH:MM');
						}
						return {type: type, value: v};
					});
				})
				.enter()
				.append("td")
					.text(function(d) { return d.value; });

	}
});


var IPDocumentationPageView = PageView.extend({
	events: {
	},
	initialize: function() {
		this.template = _.template($("#ip-documentation-template").html());
		this.flowTable = new UndocumentedTableView()
	},
	render: function() {
		$(this.el).html(this.template());
		$(".viz-undocumented-table", this.el).append(this.flowTable.el);

		return this;
	},
	show: function() {
		$(window).bind("resize", this._onResize);
		return PageView.prototype.show.call(this);
	},
	hide: function() {
		$(window).unbind("resize", this._onResize);
		return PageView.prototype.hide.call(this);
	},
	remove: function() {
		$(this.el).remove();
		return this;
	}
});
