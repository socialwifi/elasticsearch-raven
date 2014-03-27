import argparse
import datetime
import os
import queue
import socket
import sys
import threading

from elasticsearch_raven.transport import ElasticsearchTransport
from elasticsearch_raven.transport import SentryMessage


def run_server():
    args = _parse_args()
    sock = get_socket(args.ip, args.port)
    if sock:
        _run_server(sock)


def _parse_args():
    parser = argparse.ArgumentParser(description='Udp proxy server for raven')
    parser.add_argument('ip')
    parser.add_argument('port', type=int)
    return parser.parse_args()


def get_socket(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.bind((ip, int(port)))
        return sock
    except socket.gaierror:
        sys.stdout.write('Wrong hostname\n')


def _run_server(sock):
    blocking_queue = queue.Queue(maxsize=os.environ.get('QUEUE_MAXSIZE', 1000))
    sender = _get_sender(blocking_queue)
    _serve(sock, blocking_queue, sender)


def _serve(sock, blocking_queue, sender):
    sender.start()
    while True:
        data, address = sock.recvfrom(65535)
        message = SentryMessage.create_from_udp(data)
        blocking_queue.put(message)
        sys.stdout.write('{host}:{port} [{date}]\n'.format(
            host=address[0], port=address[1], date=datetime.datetime.now()))


def _get_sender(blocking_queue):
    host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
    use_ssl = bool(os.environ.get('USE_SSL', False))
    transport = ElasticsearchTransport(host, use_ssl)

    def _send():
        while True:
            message = blocking_queue.get()
            transport.send(message)
            blocking_queue.task_done()

    sender = threading.Thread(target=_send)
    return sender
