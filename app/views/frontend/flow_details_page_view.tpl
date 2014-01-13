<script type="text/template" id="flow-details-page-template">
	<div class="container row page-dashboard" style="background-color: #fff;">
		<div id="sidebar" class="col-lg-2">
		{% include "views/frontend/filter-sidebar-elements.tpl.common" %}
		</div>
		<div class="col-lg-10">
			<div class="page-header">
				<h1>Flow Details</h1>
			</div>
			<div class="viz-timeline"></div>
			<h2>Host Overview</h2>
			<ul class="nav nav-pills hostview-value">
				<li data-value="flows"><a href="javascript:void(0)">Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)">Packets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)">Bytes</a></li>
			</ul>
			<div class="viz-hostview"></div>
			<h2>Time series</h2>
			<ul class="nav nav-pills bucket-chart-value">
				<li data-value="flows"><a href="javascript:void(0)"># Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)"># Pakets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)"># Bytes</a></li>
			</ul>
			<div class="viz-buckets"></div>
			<h2>Distribution of nodes and ports</h2>
			<ul class="nav nav-pills donut-chart-value">
				<li data-value="flows"><a href="javascript:void(0)"># Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)"># Pakets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)"># Bytes</a></li>
			</ul>
			<div class="row">
				<div class="col-md-6">
					<div class="viz-donut-nodes"></div>
				</div>
				<div class="col-md-6">
					<div class="viz-donut-ports"></div>
				</div>
			</div>
		</div>
		<footer id="footbar" class="well"> 
			<ul class="nav nav-pills timeline-value">
				<li data-value="flows"><a href="javascript:void(0)"># Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)"># Packets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)"># Bytes</a></li>
			</ul>
		</footer>
	</div>
</script>
