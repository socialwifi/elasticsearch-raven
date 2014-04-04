import string
from unittest import TestCase

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
