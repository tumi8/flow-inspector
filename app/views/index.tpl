<!DOCTYPE HTML>
<html lang="en-US">
<head>
	<meta charset="UTF-8">
	<title>Flow Inspector</title>
	<meta name="description" content="Visualize and analyse traffic flows with Flow Inspector.">
    <meta name="authors" content="Mario Volke, Lothar Braun">
    
    <link href="/static/css/dev/bootstrap.css" rel="stylesheet">
    <link href="/static/css/dev/jquery.jscrollpane.css" rel="stylesheet">
    <link href="/static/css/dev/screen.css" rel="stylesheet">
</head>
<body>

	<header class="topbar">
		<div class="topbar-inner">
			<div class="container-fluid">
				<a class="brand" href="/">Flow Inspector</a>
				<ul class="nav primary-nav">
					<li class="overview"><a href="/">Overview</a></li>
					<!--<li class="pcap"><a href="/pcap">PCAP</a></li>
					<li class="query-page"><a href="/query-page">Flow Querys</a></li>-->
					<li class="dashboard"><a href="/dashboard">Dashboard</a></li>
					<li class="flow-details"><a href="/flow-details">Flow Details</a></li>
					<li class="graph"><a href="/graph">Graph</a></li>
					<li class="edge-bundle"><a href="/hierarchical-edge-bundle">Hierarchical Edge Bundle</a></li>
					<li class="hive-plot"><a href="/hive-plot">Hive Plot</a></li>
					<li class="ip-documentation"><a href="/ip-documentation">IPDB</a></li>
				</ul>
				
				<ul class="nav secondary-nav">
					<li class="dropdown">
						<a href="javascript:void(0)" class="dropdown-toggle">Actions</a>
						<ul class="dropdown-menu">
							<li><a class="export" href="javascript:void(0)">Export to SVG</a></li>
						</ul>
					</li>
				</ul>
			</div>
		</div>
	</header>
	
	<div class="alerts">
	    <!--<div class="alert-message warning">
	    	<a class="close" href="#">×</a>
	    	<p><strong>Holy guacamole!</strong> Best check yo self, you’re not looking too good.</p>
	    </div>-->
	</div>
    
    <div id="bd"></div>
    
    <div id="select-svg-overlay"><div><span>click</span><br />to save</div></div>
	
	{% for file in frontend_templates %}
		{% include file %}
	{% endfor %}
    
    {% for file in include_js %}
    	<script src="{{ file }}"></script>
    {% endfor %}
    
</body>
</html>
