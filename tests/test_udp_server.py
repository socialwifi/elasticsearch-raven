import datetime
import socket
import time
import threading
import unittest
from unittest import mock

import elasticsearch

from elasticsearch_raven.transport import SentryMessage
from elasticsearch_raven import udp_server


class RunServerTest(unittest.TestCase):
    @mock.patch('elasticsearch_raven.udp_server.ElasticsearchTransport')
    @mock.patch('queue.Queue')
    @mock.patch('argparse._sys')
    @mock.patch('elasticsearch_raven.udp_server._run_server')
    @mock.patch('elasticsearch_raven.udp_server.get_socket')
    def test_args(self, get_socket, _run_server, sys, Queue, Transport):
        Queue.side_effect = ['pending_logs', 'exception_queue']
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
    @mock.patch('sys.stdout.write')
    @mock.patch('elasticsearch_raven.udp_server._parse_args')
    def test_socket_error_handling(self, _parse_args, stdout_write, sock):
        _parse_args.return_value.ip = '192.168.1.256'
        _parse_args.return_value.port = 8888
        sock.side_effect = socket.gaierror
        self.assertRaises(SystemExit, udp_server.run_server)
        self.assertEqual([mock.call('Wrong hostname.\n')],
                         stdout_write.mock_calls)


class ParseArgsTest(unittest.TestCase):
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


class _RunServerTest(unittest.TestCase):
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

    @mock.patch('elasticsearch_raven.udp_server.get_sender')
    def test_stop_handling_on_interrupt(self, get_sender):
        self.exception_queue.get.side_effect = KeyboardInterrupt
        udp_server._run_server(self.sock, self.pending_logs,
                               self.exception_queue, self.transport)
        self.assertEqual([mock.call.recvfrom(65535), mock.call.close(),
                          mock.call.close()], self.sock.mock_calls)


class GetHandlerTest(unittest.TestCase):
    def setUp(self):
        self.sock = mock.Mock()
        self.pending_logs = mock.Mock()
        self.exception_queue = mock.Mock()
        self.exception = Exception('test')
        self.sock.recvfrom.side_effect = [({}, ('192.168.1.1', 8888)),
                                          self.exception]

    @mock.patch('elasticsearch_raven.udp_server.datetime')
    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    @mock.patch('sys.stdout.write')
    def test_debug(self, write, SentryMessage, datetime_mock):
        datetime_mock.datetime.now.return_value = datetime.datetime(2014, 1, 1)
        udp_server.get_handler(self.sock, self.pending_logs,
                               self.exception_queue, debug=True)._target()
        self.assertEqual([mock.call('192.168.1.1:8888 [2014-01-01 00:00:00]\n')],
                         write.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    def test_exception(self, SentryMessage):
        udp_server.get_handler(self.sock, self.pending_logs,
                               self.exception_queue).start()
        self.assertEqual([mock.call.put(self.exception)],
                         self.exception_queue.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.SentryMessage')
    def test_put_result_and_join_on_queue(self, SentryMessage):
        udp_server.get_handler(self.sock, self.pending_logs,
                               self.exception_queue)._target()
        self.assertEqual([mock.call.put(SentryMessage.create_from_udp()),
                          mock.call.join()], self.pending_logs.mock_calls)

    def test_daemon_thread(self):
        result = udp_server.get_handler(self.sock, self.pending_logs,
                                        self.exception_queue)
        self.assertIsInstance(result, threading.Thread)
        self.assertEqual(True, result.daemon)


class GetSenderTest(unittest.TestCase):
    def setUp(self):
        self.pending_logs = mock.Mock()
        self.exception_queue = mock.Mock()
        self.transport = mock.Mock()

    def test_exception(self):
        self.pending_logs.get.return_value = mock.Mock()
        exception = Exception('test')
        self.transport.send.side_effect = exception
        udp_server.get_sender(self.transport, self.pending_logs,
                              self.exception_queue)._target()
        self.assertEqual([mock.call.put(exception)],
                         self.exception_queue.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.retry_loop')
    def test_retry_connection(self, retry_loop):
        self.pending_logs.get.return_value = mock.Mock()
        self.pending_logs.task_done.side_effect = Exception('test')
        exception = elasticsearch.exceptions.ConnectionError('test')
        self.transport.send.side_effect = exception
        retry = mock.Mock()
        retry_loop.return_value = [retry, retry, retry]
        udp_server.get_sender(self.transport, self.pending_logs,
                              self.exception_queue)._target()
        self.assertEqual([mock.call(exception)]*3, retry.mock_calls)
        self.assertEqual([mock.call(900, delay=1, back_off=1.5)],
                         retry_loop.mock_calls)

    def test_daemon_thread(self):
        result = udp_server.get_sender(self.transport, self.pending_logs,
                                       self.exception_queue)
        self.assertIsInstance(result, threading.Thread)
        self.assertEqual(True, result.daemon)

    def test_task_done(self):
        self.pending_logs.get.return_value = mock.Mock()
        self.pending_logs.task_done.side_effect = Exception('test')
        udp_server.get_sender(self.transport, self.pending_logs,
                              self.exception_queue)._target()
        self.assertEqual([mock.call.get(), mock.call.task_done()],
                         self.pending_logs.mock_calls)

    @mock.patch('elasticsearch_raven.udp_server.elasticsearch.Elasticsearch')
    def test_log_transport_error(self, Elasticsearch):
        exception = elasticsearch.exceptions.TransportError(404, 'test')
        self.transport.send.side_effect = [exception, Exception]
        headers = {'test_header': 'foo'},
        body = {'int': 1}
        self.pending_logs.get.return_value = SentryMessage(headers, body)
        udp_server.get_sender(self.transport, self.pending_logs,
                              self.exception_queue)._target()
        self.assertEqual(
            [mock.call(http_auth=None, hosts=['localhost:9200'],
                       use_ssl=False),
             mock.call().__getattr__('index')(body={
                 'error': "TransportError(404, 'test')",
                 'message': "SentryMessage(headers=({'test_header': 'foo'},), "
                            "body={'int': 1})"},
                               doc_type='elasticsearch-raven-log',
                               index='elasticsearch-raven-error')],
            Elasticsearch.mock_calls)


class RetryLoopTest(unittest.TestCase):
    def test_timeout(self):
        start_time = time.time()
        try:
            for retry in udp_server.retry_loop(0.001, 0):
                try:
                    raise Exception('test')
                except Exception as e:
                    retry(e)
        except Exception:
            pass
        self.assertLessEqual(0.001, time.time() - start_time)

    @mock.patch('time.sleep')
    def test_delay(self, sleep):

        retry_generator = udp_server.retry_loop(10, 1)
        for i in range(4):
            retry = retry_generator.__next__()
            retry(Exception('test'))
        self.assertEqual([mock.call(1), mock.call(1), mock.call(1)],
                         sleep.mock_calls)

    @mock.patch('time.sleep')
    def test_back_off(self, sleep):
        retry_generator = udp_server.retry_loop(10, 1, back_off=2)
        for i in range(4):
            retry = retry_generator.__next__()
            retry(Exception('test'))
        self.assertEqual([mock.call(1), mock.call(2), mock.call(4)],
                         sleep.mock_calls)

    def test_raises(self):
        retry_generator = udp_server.retry_loop(0, 0)
        retry = retry_generator.__next__()
        retry(Exception('test'))
        self.assertRaises(Exception, retry_generator.__next__)
