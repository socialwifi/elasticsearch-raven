import base64

try:
    import queue
except ImportError:
    import Queue as queue

import kombu

from elasticsearch_raven import transport


class Empty(Exception):
    pass


class AbstractQueue:
    def get(self, timeout=None):
        raise NotImplementedError

    def put(self, message):
        raise NotImplementedError

    def join(self):
        raise NotImplementedError

    def task_done(self):
        raise NotImplementedError

    def has_nonpersistent_task(self):
        raise NotImplementedError


class ThreadingQueue:
    def __init__(self, *args, **kwargs):
        self.queue = queue.Queue(*args, **kwargs)

    def get(self, timeout=None):
        try:
            return self.queue.get(timeout=timeout)
        except queue.Empty:
            raise Empty()

    def put(self, message):
        self.queue.put(message)

    def join(self):
        self.queue.join()

    def task_done(self):
        self.queue.task_done()

    def has_nonpersistent_task(self):
        return bool(self.queue.unfinished_tasks)


class KombuQueue:
    def __init__(self, amqp_url, queue_name):
        self.connection = kombu.Connection(amqp_url)
        self.queue = self.connection.SimpleQueue(queue_name)
        self._processed = None

    def get(self, timeout=None):
        try:
            self._processed = self.queue.get(timeout=timeout)
        except self.queue.Empty:
            raise Empty()
        else:
            return self._deserialize(self._processed.payload)

    def put(self, message):
        return self.queue.put(self._serialize(message))

    def join(self):
        pass

    def task_done(self):
        self._processed.ack()
        self._processed = None

    def has_nonpersistent_task(self):
        return False

    def _serialize(self, message):
        return message.headers, base64.b64encode(message.body).decode('utf-8')

    def _deserialize(self, data):
        headers, encoded_body = data
        body = base64.b64decode(encoded_body.encode('utf-8'))
        return transport.SentryMessage(headers, body)
