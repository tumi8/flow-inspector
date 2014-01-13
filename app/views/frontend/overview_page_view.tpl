<script type="text/template" id="overview-page-template">
	<div class="container page-overview">
		<div class="content">
			<div class="page-header">
			<div class="container">
				<h1>Global Database Overview</h1>
			</div>
			<h2>Host Overview</h2>
			<ul class="nav nav-pills hostview-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)">Bytes</a></li>
			</ul>
			<div class="viz-hostview"></div>

			<h2>Distribution of nodes and ports</h2>
			<ul class="nav nav-pills donut-chart-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)">Bytes</a></li>
			</ul>
			<div class="row">
				<div class="col-md-6">
					<div class="viz-donut-nodes"></div>
				</div>
				<div class="col-md-6">
					<div class="viz-donut-ports"></div>
				</div>
			</div>
			<h2>Traffic Overview</h2>
			<ul class="nav nav-pills bucketview-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)">Bytes</a></li>
			</ul>
			<div class="viz-buckets"></div>
		</div>
		<footer>
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
