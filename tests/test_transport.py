import datetime
import logging
import string
from unittest import TestCase

try:
    from unitetest import mock
except ImportError:
    import mock

from elasticsearch_raven import exceptions
from elasticsearch_raven import transport


class ParseSentryHeadersTest(TestCase):
    def test_empty_string(self):
        arg = ''
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.parse_headers, arg)

    def test_empty_arguments(self):
        arg = 'sentry_key=, sentry_secret='
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.parse_headers, arg)

    def test_example(self):
        arg = ''''Sentry sentry_timestamp=1396269830.8627632,
              sentry_client=raven-python/4.0.4, sentry_version=4,
              sentry_key=public, sentry_secret=secret'''
        result = transport.SentryMessage.parse_headers(arg)
        self.assertEqual({'sentry_key': 'public',
                          'sentry_secret': 'secret'}, result)

    def test_reverse_order(self):
        arg = 'sentry_secret=b, sentry_key=a'
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.parse_headers, arg)

    def test_random_string(self):
        arg = string.printable
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.parse_headers, arg)

    def test_man_in_the_middle(self):
        arg = 'sentry_key=a, man_in_the_middle=yes, sentry_secret=b'
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.parse_headers, arg)


class DecodeBodyTest(TestCase):
    def test_empty(self):
        message = mock.Mock(transport.SentryMessage)
        message.body = b''
        self.assertRaises(exceptions.DamagedSentryMessageBodyError,
                          transport.SentryMessage.decode_body, message)

    def test_example(self):
        message = mock.Mock(transport.SentryMessage)
        message.body = b'x\x9c\xabV*)-\xc8IU\xb2R\x88\x8e\xd5QP\xca\xc9,.' \
                       b'\x81\xb1\xd3r\xf2\x13A\x1cC=\x03 /3\x0f\xcc\xae\x05' \
                       b'\x00kU\r\xcc'
        result = transport.SentryMessage.decode_body(message)
        self.assertEqual({'int': 1, 'float': 1.0, 'list': [], 'tuple': []},
                         result)

    def test_random(self):
        message = mock.Mock(transport.SentryMessage)
        message.body = b'x\x9c\xd3\xb5\x05\x00\x00\x99\x00k'
        self.assertRaises(exceptions.DamagedSentryMessageBodyError,
                          transport.SentryMessage.decode_body, message)


class CreateFromUDPTest(TestCase):
    def test_empty(self):
        arg = b''
        self.assertRaises(exceptions.DamagedSentryMessageError,
                          transport.SentryMessage.create_from_udp, arg)

    def test_separator(self):
        arg = b'\n\n'
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.create_from_udp, arg)

    def test_example(self):
        arg = b'sentry_key=a, sentry_secret=b\n\nYm9keQ=='
        message = transport.SentryMessage.create_from_udp(arg)
        self.assertEqual({'sentry_key': 'a', 'sentry_secret': 'b'},
                         message.headers)
        self.assertEqual(b'body', message.body)


class CreateFromHttpTest(TestCase):
    def test_empty(self):
        args = '', ''
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          transport.SentryMessage.create_from_http, *args)

    def test_example(self):
        args = 'sentry_key=a, sentry_secret=b', 'Ym9keQ=='
        message = transport.SentryMessage.create_from_http(*args)
        self.assertEqual({'sentry_key': 'a', 'sentry_secret': 'b'},
                         message.headers)
        self.assertEqual(b'body', message.body)


class LogTransportSendTest(TestCase):
    @mock.patch('elasticsearch_raven.transport.datetime')
    @mock.patch('elasticsearch.Elasticsearch')
    def test_example(self, ElasticSearch, datetime_mock):
        log_transport = transport.LogTransport('example.com', False)
        datetime_mock.datetime.now.return_value = datetime.datetime(2014, 1, 1)
        headers = {'sentry_key': 'key123', 'sentry_secret': 'secret456'}
        body = {'project': 'index-{0:%Y.%m.%d}', 'extra': {'foo': 'bar'}}
        message = transport.SentryMessage(headers, body)
        message.decode_body = mock.Mock()
        message.decode_body.return_value = body
        log_transport.send_message(message)
        self.assertEqual([mock.call(
            http_auth='key123:secret456', use_ssl=False,
            hosts=['example.com']),
            mock.call().__getattr__('index')(
                index='index-2014.01.01', doc_type='raven-log', body={
                    'project': 'index-{0:%Y.%m.%d}', 'extra': {
                    'foo<string>': 'bar'}},
                id='a453b51cddaaed66942291468b0cbad96f17ef72')],
            ElasticSearch.mock_calls)

    def test_get_id(self):
        arg = {'a': '1', 'b': 2, 'c': None, 'd': [], 'e': {}}
        self.assertEqual('a07adfbed45a1475e48e216e3a38e529b2e4ddcd',
                         transport.hash_dict(arg))

    def test_get_id_sort(self):
        arg1 = {'a': '1', 'b': 2, 'c': None, 'd': [], 'e': {}}
        arg2 = {'e': {}, 'd': [], 'c': None, 'b': 2, 'a': '1'}
        self.assertEqual(transport.hash_dict(arg1),
                         transport.hash_dict(arg2))




class LoggerLevelToErrorTest(TestCase):
    def test_level(self):
        logger = logging.getLogger('test')
        logger.setLevel(logging.WARNING)
        with transport.logger_level_to_error('test'):
            self.assertEqual(logging.ERROR, logger.level)
        self.assertEqual(logging.WARNING, logger.level)
