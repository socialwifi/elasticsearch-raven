import argparse
import socket
import sys
import signal

try:
    import queue
except ImportError:
    import Queue as queue

from elasticsearch_raven import configuration
from elasticsearch_raven import transport
from elasticsearch_raven import queue_sender
from elasticsearch_raven import queues
from elasticsearch_raven import udp_handler


def run_server():
    args = _parse_args()
    try:
        sock = get_socket(args.ip, args.port)
    except socket.gaierror:
        sys.stdout.write('Wrong hostname.\n')
        sys.exit(1)
    else:
        log_transport = transport.get_configured_log_transport()
        if args.amqp_queue:
            pending_logs = queues.KombuQueue(configuration['amqp_url'],
                                             configuration['amqp_queue'])
        else:
            pending_logs = queues.ThreadingQueue(
                configuration['queue_maxsize'])
        Server(sock, pending_logs, log_transport,
               args.debug).run()


def _parse_args():
    parser = argparse.ArgumentParser(description='Udp proxy server for raven')
    parser.add_argument('ip')
    parser.add_argument('port', type=int)
    parser.add_argument('--debug', dest='debug', action='store_const',
                        const=True, default=False,
                        help='Print debug logs to stdout')
    parser.add_argument(
        '--amqp-queue', action='store_const', const=True, default=False,
        help='Use amqp queue to store logs to send to elasticsearch.')
    return parser.parse_args()


def get_socket(ip, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip, int(port)))
    return sock


class Server(object):
    def __init__(self, sock, pending_logs, log_transport, debug=False):
        self.sock = sock
        self.pending_logs = pending_logs
        self.log_transport = log_transport
        self.debug = debug
        self.exception_queue = queue.Queue()

    def run(self):
        handler = udp_handler.Handler(
            self.sock, self.pending_logs, self.thread_exception_handler,
            debug=self.debug)
        sender = queue_sender.Sender(self.log_transport, self.pending_logs,
                                     self.thread_exception_handler)
        handler.as_thread().start()
        sender.as_thread().start()

        def terminate(signum, frame):
            self.exception_queue.put(KeyboardInterrupt())

        signal.signal(signal.SIGTERM, terminate)
        signal.signal(signal.SIGQUIT, terminate)
        try:
            raise self.exception_queue.get()
        except KeyboardInterrupt:
            handler.should_finish = True
            try:
                while self.pending_logs.has_nonpersistent_task():
                    try:
                        raise self.exception_queue.get(timeout=1)
                    except queue.Empty:
                        pass
            except KeyboardInterrupt:
                pass

    def thread_exception_handler(self, exception):
        self.exception_queue.put(exception)
