<script type="text/template" id="pcap-page-template">
	<div class="container page-pcap">
		<div class="content">
			<div class="page-header">
				<h1>PCAP Viewer Interface</h1>
			</div>
			<h2>Query Interface </h2>

			<form id="fileupload" class="navbar-search-pull-right" action="/pcap" method="post" enctype="multipart/form-data" >
				<span class="btn btn-success fileinput-button">
					<i class="icon-plus icon-white"></i>
					<span>Add files...</span>
					<input type="file" name="data" id="data">
				</span>
				<a class="btn btn-primary submit start"
					href="javascript:void(0)"
					title="Submit"
					data-content="Submit query to database">
                                                <i class="icon-upload icon-white"></i>
						<span>Start upload</span>
				</a>
			</form>

			<div class="pcap-live-stats"/>

			<div class="stats-images"/>

			<h2>Detailed OverView</h2>
                        <ul class="pills pcap-stat-value">
                                <li data-value="allFlows"><a href="javascript:void(0)">All Flows</a></li>
                                <li data-value="withGaps"><a href="javascript:void(0)">Flows with Gaps</a></li>
                                <li data-value="lowThroughput"><a href="javascript:void(0)">Flows with low throughput</a></li>
                        </ul>
			<div class="pcap-stats"/>
		</div>
		<footer class="well">
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
