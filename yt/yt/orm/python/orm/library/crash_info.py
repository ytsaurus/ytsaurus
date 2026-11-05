import glob
import os

from yt.orm.library.common import ClientError


class CrashInfo(object):
    DEFAULT_MAX_BYTES = 32 * 1024

    # Match only master stderr ("stderr.<name>_master"), not the embedded YT's stderr.*.
    DEFAULT_STDERR_PATTERN = "stderr.*_master"

    @classmethod
    def check_directory(cls, directory, context=None, stderr_pattern=DEFAULT_STDERR_PATTERN):
        pattern = os.path.join(directory, "**", stderr_pattern)
        for path in sorted(glob.glob(pattern, recursive=True)):
            result = cls(path, context=context)
            if result.has_crash():
                return result
        return None

    def __init__(self, stderr_file, context=None):
        self._log = stderr_file
        self._max_bytes = self.DEFAULT_MAX_BYTES
        self._report = self._read_stderr(stderr_file)
        self._context = context

    def has_crash(self):
        return bool(self._report)

    def raise_for_crash(self, context=None):
        if not self.has_crash():
            return
        parts = []
        if context:
            parts.append(context)
        if self._context:
            parts.append(self._context)
        if self._report:
            parts.append("##### Server crash output in {} #####\n{}".format(self._log, self._report))
        raise ClientError("\n".join(parts))

    def _read_stderr(self, path):
        if not path or not os.path.exists(path):
            return None
        try:
            with open(path, "r", errors="replace") as fh:
                content = fh.read(self._max_bytes)
        except OSError:
            return None
        # Any non-empty stderr indicates crash output (ASAN report, stack trace, etc.).
        if content:
            return content
        return None
