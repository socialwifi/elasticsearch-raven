import time
from unittest import TestCase
from unittest import mock

from elasticsearch_raven import utils


class RetryLoopTest(TestCase):
    @mock.patch('time.sleep')
    def test_delay(self, sleep):

        retry_generator = utils.retry_loop(1)
        for i in range(4):
            retry = next(retry_generator)
            retry(Exception('test'))
        self.assertEqual([mock.call(1), mock.call(1), mock.call(1)],
                         sleep.mock_calls)

    @mock.patch('time.sleep')
    def test_back_off(self, sleep):
        retry_generator = utils.retry_loop(1, max_delay=4, back_off=2)
        for i in range(5):
            retry = next(retry_generator)
            retry(Exception('test'))
        self.assertEqual([mock.call(1), mock.call(2), mock.call(4), mock.call(4)],
                         sleep.mock_calls)
