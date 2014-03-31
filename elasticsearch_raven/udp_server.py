import argparse
import datetime
import queue
import socket
import sys
import threading

from elasticsearch_raven import configuration
from elasticsearch_raven.transport import ElasticsearchTransport
from elasticsearch_raven.transport import SentryMessage


def run_server():
    args = _parse_args()
    try:
        sock = get_socket(args.ip, args.port)
    except socket.gaierror:
        sys.stdout.write('Wrong hostname.\n')
        sys.exit(1)
    else:
        _run_server(sock, args.debug)


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


def _run_server(sock, debug=False):
    pending_logs = queue.Queue(configuration['queue_maxsize'])
    exception_queue = queue.Queue()
    handler = _get_handler(sock, pending_logs, exception_queue, debug=debug)
    sender = _get_sender(pending_logs, exception_queue)
    handler.start()
    sender.start()
    try:
        raise exception_queue.get()
    except KeyboardInterrupt:
        pending_logs.join()


def _get_handler(sock, pending_logs, exception_queue, debug=False):
    def _handle():
        try:
            while True:
                data, address = sock.recvfrom(65535)
                message = SentryMessage.create_from_udp(data)
                pending_logs.put(message)
                if debug:
                    sys.stdout.write('{host}:{port} [{date}]\n'.format(
                        host=address[0], port=address[1],
                        date=datetime.datetime.now()))
        except Exception as e:
            pending_logs.join()
            exception_queue.put(e)

    handler = threading.Thread(target=_handle)
    handler.daemon = True
    return handler


def _get_sender(pending_logs, exception_queue):
    transport = ElasticsearchTransport(configuration['host'],
                                       configuration['use_ssl'])

    def _send():
        try:
            while True:
                message = pending_logs.get()
                transport.send(message)
                pending_logs.task_done()
        except Exception as e:
            exception_queue.put(e)

    sender = threading.Thread(target=_send)
    sender.daemon = True
    return sender
