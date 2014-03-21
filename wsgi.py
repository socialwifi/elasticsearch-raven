import os
from queue import Queue
from threading import Thread

from elasticsearch_raven.transport import ElasticsearchTransport
from elasticsearch_raven.utils import get_index

host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
transport = ElasticsearchTransport(host)
blocking_queue = Queue()


def send():
    while True:
        data, index = blocking_queue.get()
        transport.send(data, index)
        blocking_queue.task_done()


sender = Thread(target=send)
sender.daemon = True
sender.start()


def application(environ, start_response):
    index = get_index(environ)
    length = int(environ.get('CONTENT_LENGTH', '0'))
    data = environ['wsgi.input'].read(length)
    blocking_queue.put((data, index))

    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [''.encode('utf-8')]
