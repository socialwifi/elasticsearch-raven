import os

configuration = {
    'host': os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200'),
    'use_ssl': os.environ.get('USE_SSL', False),
    'queue_maxsize': os.environ.get('QUEUE_MAXSIZE', 1000),
    'http_auth': os.environ.get('ELASTICSEARCH_AUTH', None),
    'amqp_url': os.environ.get('AMQP_URL',
                               'amqp://guest:guest@localhost:5672//'),
    'amqp_queue': os.environ.get('AMQP_QUEUE', 'elasticsearch-raven-logs'),
}
