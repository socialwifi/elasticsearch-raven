import os

configuration = {
    'host': os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200'),
    'use_ssl': os.environ.get('USE_SSL', False),
    'queue_maxsize': os.environ.get('QUEUE_MAXSIZE', 1000),
    'http_auth': os.environ.get('ELASTICSEARCH_AUTH', None)
}
