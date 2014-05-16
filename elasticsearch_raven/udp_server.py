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


def run_server():
    args = _parse_args()
    try:
        sock = get_socket(args.ip, args.port)
    except socket.gaierror:
        sys.stdout.write('Wrong hostname.\n')
        sys.exit(1)
    else:
        log_transport = transport.get_configured_log_transport()
        pending_logs = queue.Queue(configuration['queue_maxsize'])
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
    handler = get_handler(sock, pending_logs, exception_queue, debug=debug)
    sender = get_sender(log_transport, pending_logs, exception_queue)
    handler.start()
    sender.start()
    try:
        raise exception_queue.get()
    except KeyboardInterrupt:
        sock.close()
        try:
            while pending_logs.unfinished_tasks:
                try:
                    raise exception_queue.get(timeout=1)
                except queue.Empty:
                    pass
        except KeyboardInterrupt:
            pass


def get_handler(sock, pending_logs, exception_queue, debug=False):
    def _handle():
        try:
            while True:
                data, address = sock.recvfrom(65535)
                message = transport.SentryMessage.create_from_udp(data)
                pending_logs.put(message)
                if debug:
                    sys.stdout.write('{host}:{port} [{date}]\n'.format(
                        host=address[0], port=address[1],
                        date=datetime.datetime.now()))
        except Exception as e:
            sock.close()
            pending_logs.join()
            exception_queue.put(e)

    handler = threading.Thread(target=_handle)
    handler.daemon = True
    return handler


def get_sender(log_transport, pending_logs, exception_queue):

    def _send():
        try:
            while True:
                _send_message(log_transport, pending_logs)
        except Exception as e:
            exception_queue.put(e)

    sender = threading.Thread(target=_send)
    sender.daemon = True
    return sender


def _send_message(log_transport, pending_logs):
    message = pending_logs.get()
    try:
        for retry in retry_loop(15 * 60, delay=1, back_off=1.5):
            try:
                log_transport.send_message(message)
            except elasticsearch.exceptions.ConnectionError as e:
                retry(e)
    except elasticsearch.exceptions.TransportError as e:
        _raport_error(message, e)
    pending_logs.task_done()


def _raport_error(message, error):
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
