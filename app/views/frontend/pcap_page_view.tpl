<script type="text/template" id="pcap-page-template">
	<div class="container page-pcap">
		<div class="content">
			<div class="page-header">
				<h1>PCAP Viewer Interface</h1>
			</div>
			<h2>Query Interface </h2>
			<form id="fileupload" class="navbar-search-pull-right" action="/pcap" method="post" enctype="multipart/form-data">
				<span class="btn btn-success fileinput-button">
					<i class="icon-plus icon-white"></i>
					<span>Add files...</span>
					<input type="file" name="data">
				</span>
				<button type="submit" class="btn btn-primary start">
					<i class="icon-upload icon-white"></i>
					<span>Start upload</span>
				</button>
				<!-- The loading indicator is shown during file processing -->
				<div class="fileupload-loading"></div>
				<br>
				<div class="span4">
					<!-- The global progress bar -->
					<div class="progress progress-success progress-striped active fade">
						<div class="bar" style="width:0%;height:30px"></div>
					</div>
				</div>
				<!-- The table listing the files available for upload/download -->
				<table role="presentation" class="table table-striped"><tbody class="files" data-toggle="modal-gallery" data-target="#modal-gallery"></tbody></table>
			</form>
			<h2>Common Stats</h2>
			<div class="stats-images"/>

			<h2>Specific Stats</h2>
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
