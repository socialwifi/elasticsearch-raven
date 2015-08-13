import contextlib
import time
import signal


def retry_loop(timeout, delay, back_off=1.0):
    start_time = time.time()
    exceptions = set()

    def retry(exception):
        exceptions.add(exception)
    yield retry
    while time.time() - start_time <= timeout:
        if not exceptions:
            return
        time.sleep(delay)
        delay *= back_off
        exceptions.clear()
        yield retry

    raise exceptions.pop()


@contextlib.contextmanager
def ignore_signals(signals):
    signal.pthread_sigmask(signal.SIG_BLOCK, signals)
    yield
    signal.pthread_sigmask(signal.SIG_UNBLOCK, signals)
