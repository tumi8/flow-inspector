<script type="text/template" id="hive-plot-page-template">
	<div class="container-fixed dark page-hive-plot">
	    <aside id="sidebar" class="well">
	    	<h5>Axis mapping</h5>
	    	<form class="form-stacked help"
	    		title="Axis mapping"
	    		data-content="The first step to render Hive plots is to map nodes onto one of three axes. Use the CIDR-notation to allocate IPv4 addresses to the axes (e.g. 192.168.0.0/16). There has to be one CIDR-address per line. If there are overlaps in IP ranges the more specific match will win (e.g. 192.168.0.0/16 matches before 192.168.0.0/8). If the same CIDR-address exist on more than one axis then nodes will be mapped to more than one axis respectively.">
	    		<fieldset>
	    			<div class="clearfix">
	    				<label for="mapAxis1">Axis 1</label>
	    				<textarea id="mapAxis1" rows="5"></textarea>
	    			</div>
	    			<div class="clearfix">
	    				<label for="mapAxis2">Axis 2</label>
	    				<textarea id="mapAxis2" rows="5"></textarea>
	    			</div>
	    			<div class="clearfix">
	    				<label for="mapAxis3">Axis 3</label>
	    				<textarea id="mapAxis3" rows="5"></textarea>
	    			</div>
	    		</fieldset>
	    	</form>
	    	<h5>Axis value and scale</h5>
	    	<form class="form-stacked">
	    		<fieldset>
	    			<div class="clearfix help"
	    				title="Numerical value"
	    				data-content="A numerical value to map nodes to a position on the axis. You can choose between the total number of flows, number of pakets or traffic in bytes corresponding to a node. Zero lies in the center of the Hive plot and the maximal value lies at the other end.">
	    				<label for="mapNumericalValue">Numerical value</label>
	    				<select id="mapNumericalValue">
	    					<option value="octetDeltaCount">Bytes</option>
	    					<option value="packetDeltaCount">Pakets</option>
	    					<option value="flows">Flows</option>
	    				</select>
	    			</div>
	    			<div class="clearfix help"
	    				title="Axis scale"
	    				data-content="Choose between linear or logarithmic scale on the axes.">
	    				<label for="mapAxisScale">Axis scale</label>
	    				<select id="mapAxisScale">
	    					<option value="linear">Linear scale</option>
	    					<option value="log">Logarithmic scale</option>
	    				</select>
	    			</div>
	    		</fieldset>
	    	</form>
	    	<h5>Flow direction</h5>
	    	<form class="form-stacked help"
	    		title="Flow direction"
	    		data-content="Choose whether both flow directions or not should be shown between two axes.">
	    		<fieldset>
	    			<div class="clearfix">
	    				<label for="directionAxis1">Axis 1 → Axis 2</label>
	    				<select id="directionAxis1">
	    					<option value="both">both</option>
	    					<option value="clockwise">clockwise</option>
	    					<option value="counter-clockwise">counter-clockwise</option>
	    				</select>
	    			</div>
	    			<div class="clearfix">
	    				<label for="directionAxis2">Axis 2 → Axis 3</label>
	    				<select id="directionAxis2">
	    					<option value="both">both</option>
	    					<option value="clockwise">clockwise</option>
	    					<option value="counter-clockwise">counter-clockwise</option>
	    				</select>
	    			</div>
	    			<div class="clearfix">
	    				<label for="directionAxis3">Axis 3 → Axis 1</label>
	    				<select id="directionAxis3">
	    					<option value="both">both</option>
	    					<option value="clockwise">clockwise</option>
	    					<option value="counter-clockwise">counter-clockwise</option>
	    				</select>
	    			</div>
	    		</fieldset>
	    	</form>
	    	<h5>Filters</h5>
	    	    	<p>
		    		<a class="btn enabled apply-filter help" 
		    			href="javascript:void(0)"
		    			title="Apply Filter" 
		    			data-content="Applies the filters defined in the fields below.">
		    			Apply Filter</a>
		    	</p>

	    	<form class="form-stacked">
	    		<fieldset>
					<div class="clearfix help"
					title="Protocol"
	    				data-content="Filter for protocols">
						<label for="filterProtocols">Protocol</label>
						<textarea id="filterProtocols" rows="10"></textarea>
						<div class="clearfix">
							<select id="filterProtocolsType">
								<option value="inclusive">include listed protocols</option>
								<option value="exclusive">exclude listed protocols</option>
							</select>
						</div>
					</div>
					<div class="help"
						title="Port filter"
						data-content="Filter flows by port numbers. Enter one port number per line. Ports can be included in the visualization which means only the listed ports will be shown. Or they can be excluded which means only flows that don't contain such a port number are shown.">
						<div class="clearfix">
							<label for="filterPorts">Ports</label>
							<textarea id="filterPorts" rows="10"></textarea>
						</div>
						<div class="clearfix">
							<select id="filterPortsType">
								<option value="inclusive">include only listed ports</option>
								<option value="exclusive">exclude listed ports</option>
							</select>
						</div>
					</div>
					<div class="help"
						title="IP address filter"
						data-content="Filter flows and node lists by IP addresses. Enter one IP address per line (no subnets are allowed at the moment). Addresses can be included in the visualization which means only the listed addresses will be shown. Or they can e excluded which means only flows that don't contain that address are shown.">
						<div class="clearfix">
							<label for="filterIPs">IP Addresses</label>
							<textarea id="filterIPs" rows="10"></textarea>
						</div>
						<div class="clearfix">
							<select id="filterIPsType">
								<option value="inclusive">include only listed IPs</option>
								<option value="exclusive">exclude listed IPs</option>
							</select>
						</div>
        	       </div>
	           </fieldset>
	       </form>
	    </aside>
	    <div id="content" class="content">
	    	<div class="canvas"></div>
	    </div>
	    <footer id="footbar" class="well">
	    		<ul class="pills timeline-value">
				<li data-value="flows"><a href="javascript:void(0)"># Flows</a></li>
				<li data-value="packetDeltaCount"><a href="javascript:void(0)"># Packets</a></li>
				<li data-value="octetDeltaCount"><a href="javascript:void(0)"># Bytes</a></li>
			</ul>
	    </footer>
	</div>
</script>
