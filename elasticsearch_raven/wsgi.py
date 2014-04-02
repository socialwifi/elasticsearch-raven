import queue

from elasticsearch_raven import configuration
from elasticsearch_raven.transport import SentryMessage
from elasticsearch_raven.udp_server import _get_sender

pending_logs = queue.Queue(configuration['queue_maxsize'])
exception_queue = queue.Queue()

sender = _get_sender(pending_logs, exception_queue)
sender.start()


def application(environ, start_response):
    try:
        exception = exception_queue.get_nowait()
    except queue.Empty:
        pass
    else:
        raise exception
    length = int(environ.get('CONTENT_LENGTH', '0'))
    data = environ['wsgi.input'].read(length)
    pending_logs.put(SentryMessage.create_from_http(
        environ['HTTP_X_SENTRY_AUTH'], data))

    status = '200 OK'
    response_headers = [('Content-Type', 'text/plain')]
    start_response(status, response_headers)
    return [''.encode('utf-8')]
