try:
    import queue
except ImportError:
    import Queue as queue


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

