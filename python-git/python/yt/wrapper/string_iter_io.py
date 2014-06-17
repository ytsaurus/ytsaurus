from itertools import chain
from cStringIO import StringIO

class StringIterIO(object):
    """
    Read-only IO stream wraps strings iterator.
    """

    def __init__(self, strings_iter, add_eoln=False):
        self._strings_iter = strings_iter
        self._sep = "\n" if add_eoln else ""
        self._pos = 0
        self._cur_string = None
        self._active = False
        self._extract_next()

    def read(self, size=None):
        """
        Get string of 'size' length from stream.

        :param size: (int) number bytes to read
        """
        if not self._active:
            return ""

        if size is None:
            self._active = False
            return self._sep.join(chain([self._cur_string[self._pos:]], self._strings_iter))
        else:
            string_output = StringIO()
            while True:
                to_write = min(size, len(self._cur_string) - self._pos)
                string_output.write(self._cur_string[self._pos : self._pos + to_write])
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
        """
        Get from string line (string ended by '\n').
        """
        if not self._active:
            return ""

        string_output = StringIO()
        while True:
            index = self._cur_string.find("\n", self._pos)
            if index != -1:
                string_output.write(self._cur_string[self._pos : index + 1])
                self._pos = index + 1
                break
            else:
                string_output.write(self._cur_string[self._pos:])
                self._pos = 0
                self._extract_next()
                if not self._active:
                    break
                string_output.write(self._sep)
                if self._sep == "\n":
                    break
        return string_output.getvalue()

    def _extract_next(self):
        try:
            self._cur_string = self._strings_iter.next()
            self._active = True
        except StopIteration:
            self._active = False
