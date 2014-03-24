import argparse
import base64
import datetime
import os
import queue
import socket
import sys
import threading

from elasticsearch_raven.transport import decode
from elasticsearch_raven.transport import ElasticsearchTransport


def run_server():
    args = _parse_args()
    _run_server(args.ip, args.port)


def _parse_args():
    parser = argparse.ArgumentParser(description='Udp proxy server for raven')
    parser.add_argument('ip')
    parser.add_argument('port', type=int)
    return parser.parse_args()


def _run_server(ip, port):
    try:
        sock = get_socket(ip, port)
    except socket.gaierror:
        sys.stdout.write('Wrong hostname\n')
        return

    blocking_queue = queue.Queue(maxsize=os.environ.get('QUEUE_MAXSIZE', 1000))
    sender = _get_sender(blocking_queue)
    sender.start()
    _serve(sock, blocking_queue)


def _serve(sock, blocking_queue):
    while True:
        data_with_header, addr = sock.recvfrom(65535)
        auth_header, data = data_with_header.split(b'\n\n')
        blocking_queue.put(base64.b64decode(data))
        sys.stdout.write('{host}:{port} [{date}]\n'.format(
            host=addr[0], port=addr[1], date=datetime.datetime.now()))


def _get_sender(blocking_queue):
    host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
    transport = ElasticsearchTransport(host)

    def _send():
        while True:
            data = blocking_queue.get()
            transport.send(decode(data))
            blocking_queue.task_done()
    sender = threading.Thread(target=_send)
    return sender


def get_socket(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, int(port)))
    return sock


