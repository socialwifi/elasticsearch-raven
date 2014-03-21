import base64
import os
from queue import Queue
from threading import Thread

from elasticsearch_raven.transport import decode
from elasticsearch_raven.transport import ElasticsearchTransport

host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
transport = ElasticsearchTransport(host)
blocking_queue = Queue()


def send():
    while True:
        data = blocking_queue.get()
        transport.send(decode(data))
        blocking_queue.task_done()


sender = Thread(target=send)
sender.start()


def application(environ, start_response):
    length = int(environ.get('CONTENT_LENGTH', '0'))
    data = environ['wsgi.input'].read(length)
    blocking_queue.put(base64.b64decode(data))

    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [''.encode('utf-8')]
