import os

from elasticsearch_raven.transport import ElasticsearchTransport
from elasticsearch_raven.utils import get_index

host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
transport = ElasticsearchTransport(host)


def application(environ, start_response):
    index = get_index(environ)
    transport.send(environ['wsgi.input'].read(), index)

    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [''.encode('utf-8')]
