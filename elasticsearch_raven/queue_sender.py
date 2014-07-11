import time
import threading

import elasticsearch

from elasticsearch_raven import configuration


class Sender(object):
    def __init__(self, log_transport, pending_logs, exception_handler):
        self.log_transport = log_transport
        self.pending_logs = pending_logs
        self.exception_handler = exception_handler

    def as_thread(self):
        sender = threading.Thread(target=self._send)
        sender.daemon = True
        return sender

    def _send(self):
        try:
            while True:
                self._send_message()
        except Exception as e:
            self.exception_handler(e)

    def _send_message(self):
        message = self.pending_logs.get()
        try:
            for retry in retry_loop(15 * 60, delay=1, back_off=1.5):
                try:
                    self.log_transport.send_message(message)
                except elasticsearch.exceptions.ConnectionError as e:
                    retry(e)
        except elasticsearch.exceptions.TransportError as e:
            self._raport_error(message, e)
        self.pending_logs.task_done()

    def _raport_error(self, message, error):
        connection = elasticsearch.Elasticsearch(
            hosts=[configuration['host']],
            use_ssl=configuration['use_ssl'],
            http_auth=configuration['http_auth'])
        body = {'message': str(message), 'error': str(error)}
        connection.index(index='elasticsearch-raven-error', body=body,
                         doc_type='elasticsearch-raven-log')


def retry_loop(timeout, delay, back_off=1.0):
    start_time = time.time()
    exceptions = set()

    def retry(exception):
        exceptions.add(exception)
    yield retry
    while time.time() - start_time <= timeout:
        if not exceptions:
            return
        time.sleep(delay)
        delay *= back_off
        exceptions.clear()
        yield retry

    raise exceptions.pop()
