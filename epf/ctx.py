import logging
import signal

log = logging.getLogger(__name__)


class DelayedKeyboardInterrupt(object):
    """
    Stuff here will not be interrupted by SIGINT
    """
    def __enter__(self):
        self.signal_received = False
        self.original_signal_handler = signal.getsignal(signal.SIGINT)
        signal.signal(signal.SIGINT, self.handler)

    def handler(self, signal, frame):
        self.signal_received = (signal, frame)
        log.debug("SIGINT received. Delaying KeyboardInterrupt.")

    def __exit__(self, type, value, traceback):
        signal.signal(signal.SIGINT, self.original_signal_handler)
        if self.signal_received:
            self.original_signal_handler(*self.signal_received)
