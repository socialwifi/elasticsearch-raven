import contextlib
import time
import signal


def retry_loop(delay, *, max_delay=None, back_off=1.0):
    exceptions = set()
    if max_delay is None:
        max_delay = delay

    def retry(exception):
        exceptions.add(exception)
    yield retry

    while delay < max_delay:
        if not exceptions:
            return
        time.sleep(delay)
        delay *= back_off
        exceptions.clear()
        yield retry

    while True:
        if not exceptions:
            return
        time.sleep(max_delay)
        exceptions.clear()
        yield retry


@contextlib.contextmanager
def ignore_signals(signals):
    signal.pthread_sigmask(signal.SIG_BLOCK, signals)
    yield
    signal.pthread_sigmask(signal.SIG_UNBLOCK, signals)
