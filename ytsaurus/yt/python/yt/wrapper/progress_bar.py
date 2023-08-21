from yt.common import update

try:
    from yt.packages import tqdm
except ImportError:
    import tqdm

import os


class CustomTqdm(tqdm.tqdm):
    # Disable the monitor thread.
    monitor_interval = 0

    def __init__(self, *args, **kwargs):
        # We use mebibytes instead of megabytes.
        kwargs = update(dict(unit="iB", unit_scale=True, ascii=True), kwargs)
        super(CustomTqdm, self).__init__(*args, **kwargs)

    @classmethod
    def format_meter(cls, n, total, *args, **kwargs):
        # NB: `super(cls)` does not support static methods, so we need `super(cls, cls)`.
        # https://stackoverflow.com/questions/26788214/super-and-staticmethod-interaction
        meter = super(cls, cls).format_meter(n, total, *args, **kwargs)
        if total:
            return meter
        else:
            # Quick way to remove colon from the progress bar
            ind = meter.find(" :")
            return meter[:ind] + meter[ind + 2:]


class SimpleProgressBar(object):
    def __init__(self, default_status, size_hint=None, filename_hint=None, enable=None):
        self.default_status = default_status
        self.size_hint = size_hint
        self.filename_hint = filename_hint
        self.enable = enable
        self._tqdm = None

    def _set_status(self, status):
        if self.filename_hint:
            self._tqdm.set_description("[{}]: {}".format(status.upper(), os.path.basename(self.filename_hint)))
        else:
            self._tqdm.set_description("[{}]".format(status.upper()))

    def start(self):
        if self.enable is None:
            disable = None
        else:
            disable = not self.enable
        self._tqdm = CustomTqdm(disable=disable, total=self.size_hint, leave=False)
        self._set_status(self.default_status)
        return self

    def finish(self, status="ok"):
        if self._tqdm is not None:
            self._set_status(status)
            self._tqdm.close()
            self._tqdm = None

    def update(self, size):
        self._tqdm.update(size)


class FakeProgressReporter(object):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        pass

    def __exit__(self, type, value, traceback):
        pass

    def wrap_stream(self, stream):
        return stream
