import datetime
import logging
import mock
import string
from unittest import TestCase

from elasticsearch_raven import exceptions
from elasticsearch_raven.transport import ElasticsearchTransport
from elasticsearch_raven.transport import logger_level_to_error
from elasticsearch_raven.transport import SentryMessage


class DummyMock(mock.Mock):
    def __eq__(self, other):
        return True


class ParseSentryHeadersTest(TestCase):
    def test_empty_string(self):
        arg = ''
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.parse_headers, arg)

    def test_empty_arguments(self):
        arg = 'sentry_key=, sentry_secret='
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.parse_headers, arg)

    def test_example(self):
        arg = ''''Sentry sentry_timestamp=1396269830.8627632,
              sentry_client=raven-python/4.0.4, sentry_version=4,
              sentry_key=public, sentry_secret=secret'''
        result = SentryMessage.parse_headers(arg)
        self.assertEqual({'sentry_key': 'public',
                          'sentry_secret': 'secret'}, result)

    def test_reverse_order(self):
        arg = 'sentry_secret=b, sentry_key=a'
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.parse_headers, arg)

    def test_random_string(self):
        arg = string.printable
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.parse_headers, arg)

    def test_man_in_the_middle(self):
        arg = 'sentry_key=a, man_in_the_middle=yes, sentry_secret=b'
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.parse_headers, arg)


class DecodeBodyTest(TestCase):
    def test_empty(self):
        message = mock.Mock(SentryMessage)
        message.body = b''
        self.assertRaises(exceptions.DamagedSentryMessageBodyError,
                          SentryMessage.decode_body, message)

    def test_example(self):
        message = mock.Mock(SentryMessage)
        message.body = b'x\x9c\xabV*)-\xc8IU\xb2R\x88\x8e\xd5QP\xca\xc9,.' \
                       b'\x81\xb1\xd3r\xf2\x13A\x1cC=\x03 /3\x0f\xcc\xae\x05' \
                       b'\x00kU\r\xcc'
        result = SentryMessage.decode_body(message)
        self.assertEqual({'int': 1, 'float': 1.0, 'list': [], 'tuple': []},
                         result)

    def test_random(self):
        message = mock.Mock(SentryMessage)
        message.body = b'x\x9c\xd3\xb5\x05\x00\x00\x99\x00k'
        self.assertRaises(exceptions.DamagedSentryMessageBodyError,
                          SentryMessage.decode_body, message)


class CreateFromUDPTest(TestCase):
    def test_empty(self):
        arg = b''
        self.assertRaises(exceptions.DamagedSentryMessageError,
                          SentryMessage.create_from_udp, arg)

    def test_separator(self):
        arg = b'\n\n'
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.create_from_udp, arg)

    def test_example(self):
        arg = b'sentry_key=a, sentry_secret=b\n\nYm9keQ=='
        message = SentryMessage.create_from_udp(arg)
        self.assertEqual({'sentry_key': 'a', 'sentry_secret': 'b'},
                         message.headers)
        self.assertEqual(b'body', message.body)


class CreateFromHttpTest(TestCase):
    def test_empty(self):
        args = '', ''
        self.assertRaises(exceptions.BadSentryMessageHeaderError,
                          SentryMessage.create_from_http, *args)

    def test_example(self):
        args = 'sentry_key=a, sentry_secret=b', 'Ym9keQ=='
        message = SentryMessage.create_from_http(*args)
        self.assertEqual({'sentry_key': 'a', 'sentry_secret': 'b'},
                         message.headers)
        self.assertEqual(b'body', message.body)


class ElasticsearchTransportSendTest(TestCase):
    @mock.patch('elasticsearch_raven.transport.datetime')
    @mock.patch('elasticsearch.Elasticsearch')
    def test_example(self, ElasticSearch, datetime_mock):
        transport = ElasticsearchTransport('example.com', False)
        datetime_mock.datetime.now.return_value = datetime.datetime(2014, 1, 1)
        headers = {'sentry_key': 'key123', 'sentry_secret': 'secret456'}
        body = {'project': 'index-{0:%Y.%m.%d}', 'extra': {'foo': 'bar'}}
        message = SentryMessage(headers, body)
        message.decode_body = mock.Mock()
        message.decode_body.return_value = body
        transport.send(message)
        self.assertEqual([mock.call(
            http_auth='key123:secret456', use_ssl=False,
            hosts=['example.com']),
            mock.call().__getattr__('index')(
                index='index-2014.01.01', doc_type='raven-log', body={
                    'project': 'index-{0:%Y.%m.%d}', 'extra': {
                    'foo<string>': 'bar'}},
                id=DummyMock())],
            ElasticSearch.mock_calls)

    def test_get_id_sort(self):
        arg1 = {'a': '1', 'b': 2, 'c': None, 'd': [], 'e': {}}
        arg2 = {'e': {}, 'd': [], 'c': None, 'b': 2, 'a': '1'}
        self.assertEqual(ElasticsearchTransport._get_id(arg1),
                         ElasticsearchTransport._get_id(arg2))


class LoggerLevelToErrorTest(TestCase):
    def test_level(self):
        logger = logging.getLogger('test')
        logger.setLevel(logging.WARNING)
        with logger_level_to_error('test'):
            self.assertEqual(logging.ERROR, logger.level)
        self.assertEqual(logging.WARNING, logger.level)
