<script type="text/template" id="query-page-template">
	<div class="container page-query">
		<div class="content">
			<div class="page-header">
				<h1>Flow Query Interface</h1>
			</div>
			<h2>Query Interface </h2>
			<table border="0" align="center">
			<tr bgcolor="#C9D5E5">
				<td colspan="3" align="center"><b>Search Form</b></td>
			</tr>
			<tr bgcolor="#ffffff">
				<td>Submit</td>
				<td>
					<a class="btn enabled submit help"
						href="javascript:void(0)"
						title="Submit"
						data-content="Submit query to database"
					>
						Submit
					</a>
				</td>
				<td>&nbsp;</td>
			</tr>
			<tr>
				<td>Start Date: </td> 
				<td>
					<input type="text"  id="qmin" VALUE="">
				</td> 
				<td><div class="query-min-flowtime"></div></td>
			</tr> 
			<tr bgcolor="#ffffff" ALIGN="LEFT">
				<td>End Date:</td> 
				<td>
					  <input type="text" id="qmax" VALUE="">
				</td>
				<td><div class="query-max-flowtime"></div></td>
			</tr> 
			<tr>
			<td>Select Time Span:</td>
			<td><div id="footbar" class="well"></div></td>
				<td></td>
			</tr>
			<tr>
				<td>IP Address:</td> 
				<td>
					<input type="text" size="20" id="ipAddress" VALUE="">
				</td>
				<td></td>
			</tr> 
			<tr>
				<td>Max Lines Displayed:</td> 
				<td>
					<input type="text" size="20" id="maxLines" VALUE="">
				</td> 
				<td><i>Eg: 200</i></td>
			</tr> 
			</table>
			<h2>Results</h2>

			<div class="query-results"></div>
		</div>
		<footer class="well">
			<p>FlowInspector</p>
		</footer>
	</div>
</script>
