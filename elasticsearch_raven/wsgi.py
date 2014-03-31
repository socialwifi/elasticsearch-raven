import os
from queue import Queue
from threading import Thread

from elasticsearch_raven import configuration
from elasticsearch_raven.transport import ElasticsearchTransport
from elasticsearch_raven.transport import SentryMessage

transport = ElasticsearchTransport(configuration['host'],
                                   configuration['use_ssl'])
blocking_queue = Queue()


def send():
    while True:
        message = blocking_queue.get()
        transport.send(message)
        blocking_queue.task_done()


sender = Thread(target=send)
sender.start()


def application(environ, start_response):
    length = int(environ.get('CONTENT_LENGTH', '0'))
    data = environ['wsgi.input'].read(length)
    blocking_queue.put(SentryMessage.create_from_http(
        environ['HTTP_X_SENTRY_AUTH'], data))

    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [''.encode('utf-8')]
