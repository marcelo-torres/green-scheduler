import time


class Stopwatch:

    def __init__(self, use_nano=False):
        self.start_time = None
        self.use_nano = use_nano

    def _get_time(self):
        return time.time_ns() if self.use_nano else time.time()

    def start(self):
        self.start_time = self._get_time()

    def get_elapsed_time(self):
        return self._get_time() - self.start_time