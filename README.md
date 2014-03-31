#elasticsearch-raven

Proxy that allows to log from raven to elasticsearch.

##Installation
###Package

elasticsearch-raven can be installed as a normal Python package.
Example installation for pip:

    $ pip install elasticsearch-raven
    
##Configuration

###settings.py

Configure Ravan to your proxy location

    RAVEN_CONFIG = {
        'dsn': 'protocol://user:password@host:port/index',
    }
user:password will be used for authentication for your elasticsearch, but even if you don't use basic http authentication Raven requires setting them, for valid Sentry dsn.

Indexes are formatted with actual date.
Suggested format:

**basic-index-{0:%Y.%m.%d}** will result in **basic-index-YYYY.MM.DD**

###Environment Variables

Define elasticsearch location by setting environment variable ELASTICSEARCH_HOST (default: 'localhost:9200')

    export ELASTICSEARCH_HOST='localhost:9100'

If you use your elasticsearch with https protocol, you should set environment variable USE_SSL to True

    export USE_SSL=True

##Usage
###Option 1: wsgi

You can run wsgi.py with any http server that supports wsgi.
Example using python simple HTTP server.

     #!/usr/bin/env python
     from elasticsearch_raven.wsgi import application
     from wsgiref.simple_server import make_server

     httpd = make_server('', 8000, application)
     httpd.serve_forever()

###Option 2: UDP server

You can run udp server with command:

    elasticsearch-raven.py host port
