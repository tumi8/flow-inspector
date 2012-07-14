<script type="text/template" id="pcap-page-template">
	<div class="container page-pcap">
		<div class="content">
			<div class="page-header">
				<h1>PCAP Viewer Interface</h1>
			</div>
			<h2>Query Interface </h2>
			<form action="/pcap" method="post" enctype="multipart/form-data">
				<input type="text" name="name" />
				<input type="file" name="data" />
				<input type="submit" name="submit" value="upload now" />
			</form>
		</div>
		<footer class="well">
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
