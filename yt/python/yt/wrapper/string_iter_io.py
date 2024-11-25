from collections.abc import Iterable
from io import BytesIO
from itertools import chain


class StringIterIO(Iterable):
    """Read-only IO stream wraps strings iterator."""

    def __init__(self, strings_iter, add_eoln=False):
        self._strings_iter = strings_iter
        self._sep = b"\n" if add_eoln else b""
        self._pos = 0
        self._cur_string = None
        self._active = False
        self._extract_next()

    def read(self, size=None):
        """Get string of "size" length from stream.

        :param int size: number bytes to read.
        """
        if not self._active:
            return b""

        if size is None:
            self._active = False
            return self._sep.join(chain([self._cur_string[self._pos:]], self._strings_iter))
        else:
            string_output = BytesIO()
            while True:
                to_write = min(size, len(self._cur_string) - self._pos)
                string_output.write(self._cur_string[self._pos: self._pos + to_write])
                self._pos += to_write
                size -= to_write
                if size == 0:
                    break
                string_output.write(self._sep)
                size -= len(self._sep)

                self._pos = 0
                self._extract_next()
                if not self._active:
                    break
            return string_output.getvalue()

    def readline(self):
        """Get from string line (string ended by "\\\\n")."""
        if not self._active:
            return b""

        string_output = BytesIO()
        while True:
            index = self._cur_string.find(b"\n", self._pos)
            if index != -1:
                string_output.write(self._cur_string[self._pos: index + 1])
                self._pos = index + 1
                break
            else:
                string_output.write(self._cur_string[self._pos:])
                self._pos = 0
                self._extract_next()
                if not self._active:
                    break
                string_output.write(self._sep)
                if self._sep == b"\n":
                    break
        return string_output.getvalue()

    def _extract_next(self):
        try:
            self._cur_string = next(self._strings_iter)
            self._active = True
        except StopIteration:
            self._active = False

    def __next__(self):
        res = self.readline()
        if not res:
            raise StopIteration()
        return res

    def __iter__(self):
        return self
