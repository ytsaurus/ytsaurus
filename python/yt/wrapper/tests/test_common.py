from yt.wrapper.common import update
from yt.wrapper.response_stream import ResponseStream, EmptyResponseStream
import random
import string

def test_update():
    assert update({"a": 10}, {"b": 20}) == {"a": 10, "b": 20}
    assert update({"a": 10}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}
    assert update({"a": 10, "b": "some"}, {"a": 20, "b": {"c": 10}}) == {"a": 20, "b": {"c": 10}}

class TestResponseStream(object):
    def test_chunk_iterator(self):
        random_line = lambda: ''.join(random.choice(string.ascii_lowercase) for _ in xrange(100))
        s = '\n'.join(random_line() for _ in xrange(3))

        class StringIterator(object):
            def __init__(self, string, chunk_size=10):
                self.string = string
                self.pos = 0
                self.chunk_size = chunk_size

            def __iter__(self):
                return self

            def next(self):
                str_part = self.string[self.pos:self.pos + self.chunk_size]
                if not str_part:
                    raise StopIteration()
                self.pos += self.chunk_size
                return str_part

        close_list = []
        def close():
            close_list.append(True)

        string_iterator = StringIterator(s, 10)
        stream = ResponseStream(lambda: s, string_iterator, close, lambda x: None, lambda: None)

        assert stream.read(20) == s[:20]
        assert stream.read(2) == s[20:22]

        iterator = stream.chunk_iter()
        assert iterator.next() == s[22:30]
        assert iterator.next() == s[30:40]

        assert stream.readline() == s[40:101]
        assert stream.readline() == s[101:202]

        assert stream.read(8) == s[202:210]

        chunks = []
        for chunk in stream.chunk_iter():
            chunks.append(chunk)

        assert len(chunks) == 10
        assert "".join(chunks) == s[210:]
        assert stream.read() == ""

        stream.close()
        assert len(close_list) > 0

    def test_empty_response_stream(self):
        stream = EmptyResponseStream()
        assert stream.read() == ""
        assert len([x for x in stream.chunk_iter()]) == 0
        assert stream.readline() == ""

