import socket
import signal

try:
    from urllib import parse
except ImportError:
    import urlparse as parse

import argparse

from elasticsearch_raven import configuration
from elasticsearch_raven import transport
from elasticsearch_raven import queues
from elasticsearch_raven import queue_sender
from elasticsearch_raven import udp_handler


def run_handler():
    args = _parse_handler_args()
    parsed = parse.urlparse(args.listen_address)
    if parsed.scheme == 'fd':
        sock = socket.fromfd(int(parsed.netloc), socket.AF_INET,
                             socket.SOCK_DGRAM)
    elif parsed.scheme == 'udp':
        ip, port = parsed.netloc.split(':')
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((ip, int(port)))
    else:
        raise ValueError('only fd and udp protocols are supported')
    sock.setblocking(1)
    sock.settimeout(None)
    queue = queues.KombuQueue(configuration['amqp_url'],
                              configuration['amqp_queue'])
    handler = udp_handler.Handler(sock, queue, _exception_handler,
                                  debug=args.debug)

    def terminate(signum, frame):
        handler.should_finish = True
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGQUIT, terminate)
    handler.handle()


def _parse_handler_args():
    parser = argparse.ArgumentParser(description='Udp proxy server for raven')
    parser.add_argument('listen_address', help='''Accepted values are fd://FD or udp://IP:PORT''')
    parser.add_argument('--debug', dest='debug', action='store_const',
                        const=True, default=False,
                        help='Print debug logs to stdout')
    return parser.parse_args()


def run_sender():
    log_transport = transport.get_configured_log_transport()
    queue = queues.KombuQueue(configuration['amqp_url'],
                              configuration['amqp_queue'])
    sender = queue_sender.Sender(log_transport, queue, _exception_handler)

    def terminate(signum, frame):
        sender.should_finish = True
    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGQUIT, terminate)
    sender.send()


def _exception_handler(exception):
    raise exception


