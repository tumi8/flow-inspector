<!DOCTYPE HTML>
<html lang="en-US">
<head>
	<meta charset="UTF-8">
	<title>Flow Inspector</title>
	<meta name="description" content="Visualize and analyse traffic flows with Flow Inspector.">
	<meta name="authors" content="Mario Volke, Lothar Braun">
	<meta name="viewport" content="width=device-width, initial-scale=1.0">
    
	<link href="/static/css/dev/bootstrap.css" rel="stylesheet">
	<link href="/static/css/dev/jquery.jscrollpane.css" rel="stylesheet">
	<link href="/static/css/dev/screen.css" rel="stylesheet">
</head>
<body>
	<div class="navbar navbar-inverse navbar-fixed-top" role="navigation">

	</div>

	<div class="navbar navbar-inverse navbar-fixed-top" role="navigation">
		<div class="container">
			<div class="navbar-header">
				<button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".navbar-collapse">
					<span class="sr-only">Toggle navigation</span>
					<span class="icon-bar"></span>
					<span class="icon-bar"></span>
					<span class="icon-bar"></span>
				</button>
				<a class="navbar-brand" href="#">Flow Inspector</a>
			</div>
			<div class="collapse navbar-collapse">
				<ul class="nav navbar-nav">
					<li class="overview"><a href="/">Overview</a></li>
					<!--<li class="query-page"><a href="/query-page">Flow Querys</a></li>-->
					<li class="dashboard"><a href="/dashboard">Dashboard</a></li>
					<li class="flow-details"><a href="/flow-details">Flow Details</a></li>
					<li class="graph"><a href="/graph">Graph</a></li>
					<li class="edge-bundle"><a href="/hierarchical-edge-bundle">Hierarchical Edge Bundle</a></li>
					<li class="hive-plot"><a href="/hive-plot">Hive Plot</a></li>
				</ul>
			</div><!--/.nav-collapse -->
		</div>
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
