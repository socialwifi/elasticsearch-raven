import argparse
import datetime
import socket
import sys
import time
import threading

try:
    import queue
except ImportError:
    import Queue as queue

import elasticsearch

from elasticsearch_raven import configuration
from elasticsearch_raven import transport
from elasticsearch_raven import queues


def run_server():
    args = _parse_args()
    try:
        sock = get_socket(args.ip, args.port)
    except socket.gaierror:
        sys.stdout.write('Wrong hostname.\n')
        sys.exit(1)
    else:
        log_transport = transport.get_configured_log_transport()
        pending_logs = queues.ThreadingQueue(configuration['queue_maxsize'])
        exception_queue = queue.Queue()
        _run_server(sock, pending_logs, exception_queue, log_transport,
                    args.debug)


def _parse_args():
    parser = argparse.ArgumentParser(description='Udp proxy server for raven')
    parser.add_argument('ip')
    parser.add_argument('port', type=int)
    parser.add_argument('--debug', dest='debug', action='store_const',
                        const=True, default=False,
                        help='Print debug logs to stdout')
    return parser.parse_args()


def get_socket(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, int(port)))
    return sock


def _run_server(sock, pending_logs, exception_queue, log_transport,
                debug=False):
    handler = Handler(sock, pending_logs, exception_queue,
                      debug=debug).as_thread()
    sender = Sender(log_transport, pending_logs, exception_queue).as_thread()
    handler.start()
    sender.start()
    try:
        raise exception_queue.get()
    except KeyboardInterrupt:
        sock.close()
        try:
            while pending_logs.has_nonpersistent_task():
                try:
                    raise exception_queue.get(timeout=1)
                except queue.Empty:
                    pass
        except KeyboardInterrupt:
            pass


class Handler(object):
    def __init__(self, sock, pending_logs, exception_queue, debug=False):
        self.sock = sock
        self.pending_logs = pending_logs
        self.exception_queue = exception_queue
        self.debug = debug

    def as_thread(self):
        handler = threading.Thread(target=self._handle)
        handler.daemon = True
        return handler

    def _handle(self):
        try:
            while True:
                data, address = self.sock.recvfrom(65535)
                message = transport.SentryMessage.create_from_udp(data)
                self.pending_logs.put(message)
                if self.debug:
                    sys.stdout.write('{host}:{port} [{date}]\n'.format(
                        host=address[0], port=address[1],
                        date=datetime.datetime.now()))
        except Exception as e:
            self.sock.close()
            self.pending_logs.join()
            self.exception_queue.put(e)


class Sender(object):
    def __init__(self, log_transport, pending_logs, exception_queue):
        self.log_transport = log_transport
        self.pending_logs = pending_logs
        self.exception_queue = exception_queue

    def as_thread(self):
        sender = threading.Thread(target=self._send)
        sender.daemon = True
        return sender

    def _send(self):
        try:
            while True:
                self._send_message()
        except Exception as e:
            self.exception_queue.put(e)

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
