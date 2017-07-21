from yt.common import YtError

from yt.packages.six import int2byte, indexbytes

class YsonError(YtError):
    pass

def _format_message(message, line_index, position, offset):
    return "{0} (Line: {1}, Position: {2}, Offset: {3})".format(message, line_index, position, offset)

def raise_yson_error(message, position_info):
    line_index, position, offset = position_info
    raise YsonError(_format_message(message, line_index, position, offset))

class StreamWrap(object):
    def __init__(self, stream, header, footer):
        self.stream = stream
        self.header = header
        self.footer = footer

        self.pos = 0
        self.state = 0

    def read(self, n):
        if n == 0:
            return self.stream.read(0)

        assert n == 1

        if self.state == 0:
            if self.pos == len(self.header):
                self.state += 1
            else:
                res = int2byte(indexbytes(self.header, self.pos))
                self.pos += 1
                return res

        if self.state == 1:
            sym = self.stream.read(1)
            if sym:
                return sym
            else:
                self.state += 1
                self.pos = 0

        if self.state == 2:
            if self.pos == len(self.footer):
                self.state += 1
            else:
                res = int2byte(indexbytes(self.footer, self.pos))
                self.pos += 1
                return res

        if self.state == 3:
            return b""
