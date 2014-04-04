import string
from unittest import TestCase

try:
    from unitetest import mock
except ImportError:
    import mock

from elasticsearch_raven import exceptions
from elasticsearch_raven.transport import SentryMessage


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
