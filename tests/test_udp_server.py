import datetime
import socket
import threading
from unittest import TestCase
from unittest import mock

import elasticsearch

from elasticsearch_raven import queue_sender
from elasticsearch_raven.transport import SentryMessage
from elasticsearch_raven import udp_handler
from elasticsearch_raven import udp_server


class RunServerTest(TestCase):
    @mock.patch('elasticsearch_raven.udp_server.transport.LogTransport')
    @mock.patch('elasticsearch_raven.udp_server.queues')
    @mock.patch('elasticsearch_raven.udp_server.queue')
    @mock.patch('argparse._sys')
    @mock.patch('elasticsearch_raven.udp_server.Server')
    @mock.patch('elasticsearch_raven.udp_server.get_socket')
    def test_args(self, get_socket, Server, sys, python_queue, queues,
                  Transport):
        queues.ThreadingQueue.return_value = 'pending_logs'
        Transport.return_value = 'transport'
        get_socket.return_value = 'test_socket'
        sys.argv = ['test', '192.168.1.1', '8888', '--debug']
        udp_server.run_server()
        self.assertEqual([mock.call('192.168.1.1', 8888)],
                         get_socket.mock_calls)
        self.assertEqual([mock.call('test_socket', 'pending_logs',
                                    'transport', True),
                          mock.call().run()],
                         Server.mock_calls)

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

    @mock.patch('elasticsearch_raven.udp_handler.Handler')
    @mock.patch('elasticsearch_raven.queue_sender.Sender')
    def test_handler_start(self, Sender, Handler):
        self.exception_queue.get.side_effect = KeyboardInterrupt
        server = udp_server.Server(self.sock, self.pending_logs, self.transport)
        server.exception_queue = self.exception_queue
        server.run()
        self.assertEqual([
            mock.call(self.sock, self.pending_logs,
                      server.thread_exception_handler, debug=False),
            mock.call().as_thread(),
            mock.call().as_thread().start()], Handler.mock_calls)

    @mock.patch('elasticsearch_raven.udp_handler.Handler')
    @mock.patch('elasticsearch_raven.queue_sender.Sender')
    def test_sender_start(self, Sender, Handler):
        self.exception_queue.get.side_effect = KeyboardInterrupt
        server = udp_server.Server(self.sock, self.pending_logs, self.transport)
        server.exception_queue = self.exception_queue
        server.run()
        self.assertEqual([mock.call(self.transport, self.pending_logs,
                                    server.thread_exception_handler),
                          mock.call().as_thread(),
                          mock.call().as_thread().start()], Sender.mock_calls)


class GetHandlerTest(TestCase):
    def setUp(self):
        self.sock = mock.Mock()
        self.pending_logs = mock.Mock()
        self.exception_queue = mock.Mock()
        self.exception = Exception('test')
        self.sock.recvfrom.side_effect = [({}, ('192.168.1.1', 8888)),
                                          self.exception]

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    @mock.patch('elasticsearch_raven.udp_handler.datetime')
    @mock.patch('elasticsearch_raven.udp_server.transport.SentryMessage')
    @mock.patch('sys.stdout')
    def test_debug(self, stdout, SentryMessage, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2014, 1, 1)
        self.run_handler_function(debug=True)

        self.assertEqual(
            [mock.call.write('192.168.1.1:8888 [2014-01-01 00:00:00]\n')],
            stdout.mock_calls)

    def run_handler_function(self, **kwargs):
        handler = udp_handler.Handler(
            self.sock, self.pending_logs, self.exception_queue, **kwargs)
        thread = handler.as_thread()
        if hasattr(thread, '_target'):
            thread._target()
        else:
            thread._Thread__target()

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    @mock.patch('elasticsearch_raven.udp_server.transport.SentryMessage')
    def test_exception(self, SentryMessage):
        self.run_handler_function()

        self.assertEqual([mock.call.put(self.exception)],
                         self.exception_queue.mock_calls)

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    @mock.patch('elasticsearch_raven.udp_server.transport.SentryMessage')
    def test_put_result_and_join_on_queue(self, SentryMessage):
        self.run_handler_function()
        self.assertEqual([mock.call.put(SentryMessage.create_from_udp())],
                         self.pending_logs.mock_calls)

    def test_daemon_thread(self):
        result = udp_handler.Handler(self.sock, self.pending_logs,
                                     self.exception_queue).as_thread()
        self.assertIsInstance(result, threading.Thread)
        self.assertEqual(True, result.daemon)

    @mock.patch('elasticsearch_raven.udp_server.transport.SentryMessage')
    def test_close_socket(self, SentryMessage):
        self.sock.recvfrom.side_effect = Exception
        self.run_handler_function()

        self.assertEqual([mock.call.recvfrom(65535), mock.call.close()],
                         self.sock.mock_calls)


class GetSenderTest(TestCase):
    def setUp(self):
        self.pending_logs = mock.Mock()
        self.exception_queue = mock.Mock()
        self.transport = mock.Mock()

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    def test_exception(self):
        self.pending_logs.get.return_value = mock.Mock()
        exception = Exception('test')
        self.transport.send_message.side_effect = exception
        self.run_sender_function()
        self.assertEqual([mock.call.put(exception)],
                         self.exception_queue.mock_calls)

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    @mock.patch('elasticsearch_raven.utils.retry_loop')
    def test_retry_connection(self, retry_loop):
        self.pending_logs.get.side_effect = [mock.Mock(), Exception]
        exception = elasticsearch.exceptions.ConnectionError('test')
        self.transport.send_message.side_effect = exception
        retry = mock.Mock()
        retry_loop.return_value = [retry, retry, retry]
        self.run_sender_function()
        self.assertEqual([mock.call(exception)]*3, retry.mock_calls)
        self.assertEqual([mock.call(900, delay=1, back_off=1.5)],
                         retry_loop.mock_calls)

    def run_sender_function(self):
        thread = queue_sender.Sender(self.transport, self.pending_logs,
                                     self.exception_queue).as_thread()
        if hasattr(thread, '_target'):
            thread._target()
        else:
            thread._Thread__target()

    def test_daemon_thread(self):
        result = queue_sender.Sender(self.transport, self.pending_logs,
                                     self.exception_queue).as_thread()
        self.assertIsInstance(result, threading.Thread)
        self.assertEqual(True, result.daemon)

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    def test_task_done(self):
        self.pending_logs.get.return_value = mock.Mock()
        self.pending_logs.task_done.side_effect = Exception('test')
        self.run_sender_function()
        self.assertEqual([mock.call.get(), mock.call.task_done()],
                         self.pending_logs.mock_calls)

    @mock.patch('elasticsearch_raven.utils.signal', mock.Mock())
    @mock.patch('elasticsearch.Elasticsearch')
    def test_log_transport_error(self, Elasticsearch):
        exception = elasticsearch.exceptions.TransportError(404, 'test')
        self.transport.send_message.side_effect = [exception, Exception]
        headers = {'test_header': 'foo'},
        body = {'int': 1}
        self.pending_logs.get.return_value = SentryMessage(headers, body)
        self.run_sender_function()
        self.assertEqual(
            [mock.call(use_ssl=False, http_auth=None,
                       hosts=['localhost:9200']),
             mock.call().__getattr__('index')(
                 body={
                     'error': "TransportError(404, 'test')",
                     'message': "SentryMessage(headers=({'test_header': 'foo'}"
                     ",), body={'int': 1})"},
             doc_type='elasticsearch-raven-log',
             index='elasticsearch-raven-error')],
            Elasticsearch.mock_calls)
