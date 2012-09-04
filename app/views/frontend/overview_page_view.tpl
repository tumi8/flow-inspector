<script type="text/template" id="overview-page-template">
	<div class="container page-overview">
		<div class="content">
			<div class="page-header">
				<h1>Overview</h1>
			</div>
			<h2>Host Overview</h2>
			<ul class="pills hostview-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="pkts"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="bytes"><a href="javascript:void(0)">Bytes</a></li>
			</ul>
			<div class="viz-hostview"></div>
			<h2>Distribution of nodes and ports</h2>
			<ul class="pills donut-chart-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="pkts"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="bytes"><a href="javascript:void(0)">Bytes</a></li>
			</ul>
			<div class="row">
				<div class="span8">
					<div class="viz-donut-nodes"></div>
				</div>
				<div class="span8">
					<div class="viz-donut-ports"></div>
				</div>
			</div>
			<h2>Traffic Overview</h2>
			<ul class="pills bucketview-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="pkts"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="bytes"><a href="javascript:void(0)">Bytes</a></li>
			<div class="viz-buckets"></div>
		</div>
		<footer>
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
