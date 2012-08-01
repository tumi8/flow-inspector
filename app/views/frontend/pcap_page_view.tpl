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

			<div class="stats-images">
				<div id="y_axis"/>
				<div id="packetChart"/>
				<div id="byteChart"/>
			</div>

			<h2>Detailed Overview</h2>
                        <ul class="pills pcap-stat-value">
                                <li data-value="allFlows"><a href="javascript:void(0)">All Flows</a></li>
                                <li data-value="withGaps"><a href="javascript:void(0)">Flows with Gaps</a></li>
                                <li data-value="lowThroughput"><a href="javascript:void(0)">Flows with low throughput</a></li>
                        </ul>

			<table border="0" align="center">
			<tr bgcolor="#C9D5E5">
				<td colspan="3" align="center"><b>Apply Filter</b></td>
			</tr>
			<tr bgcolor="#ffffff">
				<td></td>
				<td>
					<a class="btn enabled apply help"
						href="javascript:void(0)"
						title="Apply"
						data-content="Apply filter"
					>
						Apply
					</a>
				</td>
			</tr>
			<tr>
				<td>Maximum number of entries to show:</td> 
				<td>
					<input type="text"  id="maxEntries" VALUE="">
				</td> 
			</tr> 
			<tr bgcolor="#ffffff" ALIGN="LEFT">
				<td>IP address / subnet</td> 
				<td>
					  <input type="text" id="ipFilter" VALUE="">
				</td>
			</tr> 
			<tr bgcolor="#ffffff" ALIGN="LEFT">
				<td>Port</td> 
				<td>
					  <input type="text" id="portFilter" VALUE="">
				</td>
			</tr> 
			</table>
			<div class="pcap-stats"/>
		</div>
		<footer class="well">
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
