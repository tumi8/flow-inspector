var FlowTableView = Backbone.View.extend({
	events: {},
	className: "flowtableview",
	initialize: function(options) {
		this.model = options.model;

		if (!this.model) {
			this.model = new FlowTableModel();
		}
		this.model.bind("change:value", this.render, this);

		this.flows = options.flows;
		this.flows.bind("reset", this.render, this);

		this.loaderTemplate = _.template($("#loader-template").html());

	},
	render: function() {
		var container = $(this.el).empty()
			w = container.width(),
			h = container.height(),
			data = this.flows.models
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
						return {type: type, value: row.attributes[type]};
					});
				})
				.enter()
				.append("td")
					.text(function(d) { return d.value; });

	}
});
