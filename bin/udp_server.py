#!/usr/bin/env python3
from datetime import datetime
import os
from queue import Queue
import socket
import sys
from threading import Thread

from elasticsearch_raven.transport import ElasticsearchTransport


blocking_queue = Queue(maxsize=os.environ.get('QUEUE_MAXSIZE', 1000))
host = os.environ.get('ELASTICSEARCH_HOST', 'localhost:9200')
transport = ElasticsearchTransport(host)


def send():
    while True:
        data = blocking_queue.get()
        auth_header, data = data.split(b'\n\n')
        transport.send(data)
        blocking_queue.task_done()


def main(*args):
    try:
        udp_ip = args[1]
        udp_port = int(args[2])
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((udp_ip, udp_port))
    except socket.gaierror:
        sys.stdout.write('Wrong hostname')
    except ValueError:
        sys.stdout.write('Wrong port')
    except IndexError:
        sys.stdout.write('arguments: hostname port')
    sender = Thread(target=send)
    sender.start()

    while True:
        data, addr = sock.recvfrom(65535)
        blocking_queue.put(data)
        sys.stdout.write('{host}:{port} [{date}]\n'.format(
            host=addr[0], port=addr[1], date=datetime.now()))


if __name__ == '__main__':
    main(*sys.argv)

