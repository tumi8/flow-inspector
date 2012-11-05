Flow Inspector
=================

Flow Inpsector is an application based on web technologies to
visualize network flows (Netflow, IPFIX).

<img src="http://webholics.github.com/flow-inspector/images/screenshot1.png" alt="Dashboard screenshot" width="200" />
<img src="http://webholics.github.com/flow-inspector/images/screenshot2.png" alt="Node-Flow-Graph screenshot" width="200" />
<img src="http://webholics.github.com/flow-inspector/images/screenshot3.png" alt="Hierarchical edge bundles screenshot" width="200" />
<img src="http://webholics.github.com/flow-inspector/images/screenshot4.png" alt="Hive Plot screenshot" width="200" />

Requirements
---------------

### App

- Python (tested with v2.7.1)
- Jinja2 (tested with v2.6)
  pip install Jinja2
- ujson (tested with 1.18) (optional but recommended)
  pip install ujson

- MongoDB (minimum requirement with v2.0)
- pymongo (tested with v2.0.1)
  pip install pymongo

### Preprocess

- Python (tested with v2.7.1)
- Redis (tested with v2.4.3)
  pip install redis

### Backends

You can choose between multiple backends for storing flow data. Additional requirements come from your choice of flow data backend. flow-inspector currently supports three backends:

- MongoDB
- MySQL 
- Oracle

Depending on your backend you have the following additional requirements:

## MongoDB
- MongoDB (minimum required: version 2.2)
- pymongo (minimum required: version 2.2)
  pip install pymongo

## MySQL
- MySQLdb (tested with v1.2.3)
  pip install mysql-python

## Oracle
- cx_oracle

Installation
---------------

- Copy /config.default.py to /config.py and edit settings
- Run /preprocess/preprocess.py to add flows to the database
- Run /app/app.py to start the web interface

Detailed information on how to install and configure can be found in the Wiki at https://github.com/constcast/flow-inspector/wiki

License
-------------

The MIT License (MIT)
Copyright (c) 2012 Mario Volke, Lothar Braun 

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
