#!/usr/bin/env python3
import base64
from datetime import datetime
import os
from queue import Queue
import socket
import sys
from threading import Thread

from elasticsearch_raven.transport import decode
from elasticsearch_raven.transport import ElasticsearchTransport


blocking_queue = Queue(maxsize=os.environ.get('QUEUE_MAXSIZE', 1000))
host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
transport = ElasticsearchTransport(host)


def send():
    while True:
        data = blocking_queue.get()
        transport.send(decode(data))
        blocking_queue.task_done()


def main(*args):
    sock = get_socket(args[1], args[2])
    sender = Thread(target=send)
    sender.start()

    while True:
        data_with_header, addr = sock.recvfrom(65535)
        auth_header, data = data_with_header.split(b'\n\n')
        blocking_queue.put(base64.b64decode(data))
        sys.stdout.write('{host}:{port} [{date}]\n'.format(
            host=addr[0], port=addr[1], date=datetime.now()))


def get_socket(ip, port):
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((ip, int(port)))
    except socket.gaierror:
        sys.stdout.write('Wrong hostname')
    except ValueError:
        sys.stdout.write('Wrong port')
    except IndexError:
        sys.stdout.write('arguments: hostname port')
    return sock


if __name__ == '__main__':
    main(*sys.argv)

