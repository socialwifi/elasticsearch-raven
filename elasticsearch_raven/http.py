try:
    import queue
except ImportError:
    import Queue as queue

from elasticsearch_raven import configuration
from elasticsearch_raven import transport
from elasticsearch_raven.queue_sender import Sender


class HttpUtils:
    def __init__(self):
        self._pending_logs = queue.Queue(configuration['queue_maxsize'])
        self._exception_queue = queue.Queue()

    def start_sender(self):
        log_transport = transport.get_configured_log_transport()
        sender = Sender(log_transport, self._pending_logs,
                        self._exception_queue.put).as_thread()
        sender.start()

    def get_application(self):
        def application(environ, start_response):
            try:
                exception = self._exception_queue.get_nowait()
            except queue.Empty:
                pass
            else:
                raise exception
            length = int(environ.get('CONTENT_LENGTH', '0'))
            data = environ['wsgi.input'].read(length)
            self._pending_logs.put(transport.SentryMessage.create_from_http(
                environ['HTTP_X_SENTRY_AUTH'], data))

            status = '200 OK'
            response_headers = [('Content-Type', 'text/plain')]
            start_response(status, response_headers)
            return [''.encode('utf-8')]
        return application
