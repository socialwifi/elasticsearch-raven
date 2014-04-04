import datetime
import socket
import threading
from unittest import TestCase

try:
    from unitetest import mock
except ImportError:
    import mock

from elasticsearch_raven import udp_server


class RunServerTest(TestCase):
    @mock.patch('elasticsearch_raven.udp_server.ElasticsearchTransport')
    @mock.patch('elasticsearch_raven.udp_server.queue')
    @mock.patch('argparse._sys')
    @mock.patch('elasticsearch_raven.udp_server._run_server')
    @mock.patch('elasticsearch_raven.udp_server.get_socket')
    def test_args(self, get_socket, _run_server, sys, queue, Transport):
        queue.Queue.side_effect = ['pending_logs', 'exception_queue']
        Transport.return_value = 'transport'
        get_socket.return_value = 'test_socket'
        sys.argv = ['test', '192.168.1.1', '8888', '--debug']
        udp_server.run_server()
        self.assertEqual([mock.call('192.168.1.1', 8888)],
                         get_socket.mock_calls)
        self.assertEqual([mock.call('test_socket', 'pending_logs',
                                    'exception_queue', 'transport', True)],
                         _run_server.mock_calls)

    @mock.patch('socket.socket')
    @mock.patch('sys.stdout')
    @mock.patch('elasticsearch_raven.udp_server._parse_args')
    def test_socket_error_handling(self, _parse_args, stdout, sock):
        _parse_args.return_value.ip = '192.168.1.256'
        _parse_args.return_value.port = 8888
        sock.side_effect = socket.gaierror
        self.assertRaises(SystemExit, udp_server.run_server)
        self.assertEqual([mock.call.write('Wrong hostname.\n')],
                         stdout.mock_calls)


class ParseArgsTest(TestCase):
    @mock.patch('argparse._sys')
    def test_ip(self, sys):
        sys.argv = ['test', '192.168.1.1', '8888']
        results = udp_server._parse_args()
        self.assertEqual('192.168.1.1', results.ip)

    @mock.patch('argparse._sys')
    def test_port(self, sys):
        sys.argv = ['test', '192.168.1.1', '8888']
        results = udp_server._parse_args()
        self.assertEqual(8888, results.port)

    @mock.patch('argparse._sys')
    def test_debug(self, sys):
        sys.argv = ['test', '192.168.1.1', '8888', '--debug']
        results = udp_server._parse_args()
        self.assertEqual(True, results.debug)


class _RunServerTest(TestCase):
    def setUp(self):
        self.sock = mock.Mock()
        self.pending_logs = mock.Mock()
        self.exception_queue = mock.Mock()
        self.transport = mock.Mock()

    @mock.patch('elasticsearch_raven.udp_server.get_handler')
    @mock.patch('elasticsearch_raven.udp_server.get_sender')
    def test_handler_start(self, get_sender, get_handler):
        self.exception_queue.get.side_effect = KeyboardInterrupt
        udp_server._run_server(self.sock, self.pending_logs,
                               self.exception_queue, self.transport)
        self.assertEqual([mock.call(
            self.sock, self.pending_logs, self.exception_queue, debug=False),
            mock.call().start()], get_handler.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.get_handler')
    @mock.patch('elasticsearch_raven.udp_server.get_sender')
    def test_sender_start(self, get_sender, get_handler):
        self.exception_queue.get.side_effect = KeyboardInterrupt
        udp_server._run_server(self.sock, self.pending_logs,
                               self.exception_queue, self.transport)

        self.assertEqual([mock.call(self.transport, self.pending_logs,
                                    self.exception_queue),
                          mock.call().start()], get_sender.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.get_handler')
    @mock.patch('elasticsearch_raven.udp_server.get_sender')
    def test_close_socket(self, get_sender, get_handler):
        self.exception_queue.get.side_effect = KeyboardInterrupt
        udp_server._run_server(self.sock, self.pending_logs,
                               self.exception_queue, self.transport)
        print(self.exception_queue.mock_calls)
        self.assertEqual([mock.call.close()], self.sock.mock_calls)


class GetHandlerTest(TestCase):
    def setUp(self):
        self.sock = mock.Mock()
        self.pending_logs = mock.Mock()
        self.exception_queue = mock.Mock()
        self.exception = Exception('test')
        self.sock.recvfrom.side_effect = [({}, ('192.168.1.1', 8888)),
                                          self.exception]

    @mock.patch('elasticsearch_raven.udp_server.datetime')
    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    @mock.patch('sys.stdout')
    def test_debug(self, stdout, SentryMessage, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2014, 1, 1)
        self.run_handler_function(debug=True)

        self.assertEqual(
            [mock.call.write('192.168.1.1:8888 [2014-01-01 00:00:00]\n')],
            stdout.mock_calls)

    def run_handler_function(self, **kwargs):
        thread = udp_server.get_handler(self.sock, self.pending_logs,
                                        self.exception_queue, **kwargs)
        if hasattr(thread, '_target'):
            thread._target()
        else:
            thread._Thread__target()

    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    def test_exception(self, SentryMessage):
        self.run_handler_function()

        self.assertEqual([mock.call.put(self.exception)],
                         self.exception_queue.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    def test_put_result_and_join_on_queue(self, SentryMessage):
        self.run_handler_function()
        self.assertEqual([mock.call.put(SentryMessage.create_from_udp()),
                          mock.call.join()], self.pending_logs.mock_calls)

    def test_daemon_thread(self):
        result = udp_server.get_handler(self.sock, self.pending_logs,
                                        self.exception_queue)
        self.assertIsInstance(result, threading.Thread)
        self.assertEqual(True, result.daemon)

    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    def test_close_socket(self, SentryMessage):
        self.sock.recvfrom.side_effect = Exception
        self.run_handler_function()

        self.assertEqual([mock.call.recvfrom(65535), mock.call.close()],
                         self.sock.mock_calls)
