from unittest import TestCase
import time
import six

try:
    from unitetest import mock
except ImportError:
    import mock

from elasticsearch_raven import utils


class RetryLoopTest(TestCase):
    def test_timeout(self):
        start_time = time.time()
        try:
            for retry in utils.retry_loop(0.001, 0):
                try:
                    raise Exception('test')
                except Exception as e:
                    retry(e)
        except Exception:
            pass
        self.assertLessEqual(0.001, time.time() - start_time)

    @mock.patch('time.sleep')
    def test_delay(self, sleep):

        retry_generator = utils.retry_loop(10, 1)
        for i in range(4):
            retry = six.next(retry_generator)
            retry(Exception('test'))
        self.assertEqual([mock.call(1), mock.call(1), mock.call(1)],
                         sleep.mock_calls)


    @mock.patch('time.sleep')
    def test_back_off(self, sleep):
        retry_generator = utils.retry_loop(10, 1, back_off=2)
        for i in range(4):
            retry = six.next(retry_generator)
            retry(Exception('test'))
        self.assertEqual([mock.call(1), mock.call(2), mock.call(4)],
                         sleep.mock_calls)

    def test_raises(self):
        retry_generator = utils.retry_loop(0, 0)
        retry = six.next(retry_generator)
        retry(Exception('test'))
        self.assertRaises(Exception, six.next, retry_generator)
