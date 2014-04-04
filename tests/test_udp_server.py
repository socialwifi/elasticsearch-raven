import socket
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
