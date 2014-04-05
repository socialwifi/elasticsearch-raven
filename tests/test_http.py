from unittest import TestCase

try:
    from unitetest import mock
except ImportError:
    import mock

from elasticsearch_raven.http import HttpUtils


class StartSenderTest(TestCase):
    @mock.patch('elasticsearch_raven.http.ElasticsearchTransport')
    @mock.patch('elasticsearch_raven.http.get_sender')
    def test_thread_start(self, get_sender, ElasticsearchTransport):
        utils = HttpUtils()
        utils.start_sender()
        self.assertEqual([mock.call(ElasticsearchTransport(),
                                    utils._pending_logs,
                                    utils._exception_queue),
                          mock.call().start()], get_sender.mock_calls)

    @mock.patch.dict('elasticsearch_raven.http.configuration', {
        'host': 'test_host', 'use_ssl': True})
    @mock.patch('elasticsearch_raven.http.ElasticsearchTransport')
    @mock.patch('elasticsearch_raven.http.get_sender')
    def test_configuration(self, get_sender, ElasticsearchTransport):
        utils = HttpUtils()
        utils.start_sender()
        self.assertEqual([mock.call('test_host', True)], ElasticsearchTransport.mock_calls)


class GetApplicationTest(TestCase):
    def setUp(self):
        self.environ = {
            'HTTP_X_SENTRY_AUTH': mock.Mock(),
            'wsgi.input': mock.Mock()}
        self.start_response = mock.Mock()

    def test_exception(self):
        utils = HttpUtils()
        aplication = utils.get_application()
        utils._exception_queue.put(Exception('test'))
        self.assertRaises(Exception, aplication, None, None)

    @mock.patch('elasticsearch_raven.http.SentryMessage')
    def test_read_content_length(self, SentryMessage):
        self.environ['CONTENT_LENGTH'] = '1234'
        utils = HttpUtils()
        aplication = utils.get_application()
        aplication(self.environ, self.start_response)
        self.assertEqual([mock.call.read(1234)],
                         self.environ['wsgi.input'].mock_calls)

    @mock.patch('elasticsearch_raven.http.SentryMessage')
    def test_create_from_http(self, SentryMessage):
        utils = HttpUtils()
        aplication = utils.get_application()
        aplication(self.environ, self.start_response)
        self.assertEqual([mock.call.create_from_http(
            self.environ['HTTP_X_SENTRY_AUTH'],
            self.environ['wsgi.input'].read())], SentryMessage.mock_calls)

    @mock.patch('elasticsearch_raven.http.SentryMessage')
    def test_put_on_pedding_logs(self, SentryMessage):
        utils = HttpUtils()
        utils._pending_logs = mock.Mock()
        aplication = utils.get_application()
        aplication(self.environ, self.start_response)
        self.assertEqual([mock.call.put(SentryMessage.create_from_http())],
                         utils._pending_logs.mock_calls)

    @mock.patch('elasticsearch_raven.http.SentryMessage')
    def test_response(self, SentryMessage):
        utils = HttpUtils()
        utils._pending_logs = mock.Mock()
        aplication = utils.get_application()
        aplication(self.environ, self.start_response)
        self.assertEqual([mock.call('200 OK',
                                    [('Content-Type', 'text/plain')])],
                         self.start_response.mock_calls)

    @mock.patch('elasticsearch_raven.http.SentryMessage')
    def test_return(self, SentryMessage):
        utils = HttpUtils()
        utils._pending_logs = mock.Mock()
        aplication = utils.get_application()
        result = aplication(self.environ, self.start_response)
        self.assertEqual([b''], result)
