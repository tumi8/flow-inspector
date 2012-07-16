<script type="text/template" id="pcap-page-template">
	<div class="container page-pcap">
		<div class="content">
			<div class="page-header">
				<h1>PCAP Viewer Interface</h1>
			</div>
			<h2>Query Interface </h2>
			<form action="/pcap" method="post" enctype="multipart/form-data">
				<input type="file" name="data" />
				<input type="submit" name="submit" value="upload now" />
			</form>
			<div class="stats-pictures"/>
                        <ul class="pills pcap-stat-value">
                                <li data-value="all"><a href="javascript:void(0)">All Flows</a></li>
                                <li data-value="gaps"><a href="javascript:void(0)">Flows with Gaps</a></li>
                                <li data-value="low-throughput"><a href="javascript:void(0)">Flows with low throughput</a></li>
                        </ul>
			<div class="pcap-stats"/>
		</div>
		<footer class="well">
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
