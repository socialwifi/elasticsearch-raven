import datetime
import sys
import signal
import threading

from elasticsearch_raven import transport
from elasticsearch_raven import utils


class Handler(object):
    def __init__(self, sock, pending_logs, exception_handler, debug=False):
        self.sock = sock
        self.pending_logs = pending_logs
        self.exception_handler = exception_handler
        self.debug = debug
        self.should_finish = False

    def as_thread(self):
        handler = threading.Thread(target=self.handle)
        handler.daemon = True
        return handler

    def handle(self):
        try:
            try:
                while True:
                    data, address = self.sock.recvfrom(65535)
                    with utils.ignore_signals([signal.SIGTERM, signal.SIGQUIT]):
                        message = transport.SentryMessage.create_from_udp(data)
                        self.pending_logs.put(message)
                    if self.debug:
                        sys.stdout.write('{host}:{port} [{date}]\n'.format(
                            host=address[0], port=address[1],
                            date=datetime.datetime.now()))
            finally:
                self.sock.close()
        except Exception as e:
            self.exception_handler(e)
