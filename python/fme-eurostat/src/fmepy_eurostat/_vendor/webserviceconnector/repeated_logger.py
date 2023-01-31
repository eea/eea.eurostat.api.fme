import threading
import time


class RepeatedLogger(object):
    """
    Non-blocking context manager class to log a message repeatedly every x seconds.
    """

    def __init__(self, logger, message, interval=30):
        """
        :param FMELoggerAdaptor logger:
        :param str message: string to log
        :param interval: seconds between calls
        """
        self._interval = interval
        self._timer = None
        self._logger = logger
        self._message = message
        self._is_running = False
        self._next_call = None
        self._periods = " "

    def __enter__(self):
        self._start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Stop the timer.
        """
        self._timer.cancel()
        self._is_running = False

    def _start(self):
        """
        Start the timer. The first call will be after x seconds.
        """
        self._next_call = time.time()
        if not self._is_running:
            self._next_call += self._interval
            self._timer = threading.Timer(self._next_call - time.time(), self._run)
            self._timer.start()
            self._is_running = True

    def _run(self):
        self._periods += "."
        self._is_running = False
        self._start()
        self._logger.info(self._message + self._periods)
