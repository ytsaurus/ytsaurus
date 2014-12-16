import fennel

from tornado import iostream
from tornado import testing
from tornado import gen

import pytest

import datetime
import subprocess
import functools
import logging
import uuid
import sys

pytestmark = pytest.mark.skipif("sys.version_info < (2,7)", reason="requires python2.7")

KAFKA_ENDPOINT = "fake.stat.yandex.net"
TEST_SERVICE_ID = "fenneltest"


def test_compression_external():
    p = subprocess.Popen(["gzip", "-d"], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdoutdata, stderrdata = p.communicate(fennel.gzip_compress("Hello"))
    assert stdoutdata == "Hello"
    assert not stderrdata


def test_compression_internal():
    assert fennel.gzip_decompress(fennel.gzip_compress("Hello")) == "Hello"


def test_event_log_timestamp_parse():
    d = datetime.datetime.strptime("2014-10-02T15:31:24.887386Z".split(".")[0], "%Y-%m-%dT%H:%M:%S")
    assert d.hour == 15
    assert d.minute == 31
    assert d.second == 24
    assert d.date().year == 2014
    assert d.date().month == 10
    assert d.date().day == 2


def test_normalize_timestamp():
    assert fennel.normalize_timestamp("2014-10-02T15:31:24.887386Z")[0] == "2014-10-02 15:31:24"
    assert fennel.normalize_timestamp("2014-10-10T14:07:22.295882Z")[0] == "2014-10-10 14:07:22"

def test_normalize_revert():
    assert fennel.revert_timestamp(*fennel.normalize_timestamp("2014-10-02T15:31:24.887386Z")) == "2014-10-02T15:31:24.887386Z"


def compare_types(left, right):
    if isinstance(left, basestring) and isinstance(right, basestring):
        pass
    else:
        assert type(left) == type(right)


def compare(left, right):
    compare_types(left, right)
    if isinstance(left, dict):
        assert len(left.keys()) == len(right.keys())
        for k in left.keys():
            compare(left[k], right[k])
    else:
        assert left == right


def test_convert_to_from():
    dataset = [
        dict(key1="value1", key2="value2"),
        dict(ks="string", ki=3),
        dict(key=dict(s1="v1", s2="v2"), other_key="data")
#        dict(key="{}")
    ]
    for data in dataset:
        data_after = fennel.convert_from_logbroker_format(fennel.convert_to_logbroker_format(data))
        compare(data, data_after)


class WorldSerialization(object):
    """Represents a world-wide serialization of events"""

    log = logging.getLogger("WorldSerialization")

    def __init__(self):
        self._description = []
        self._index = 0
        self._index_change_futures = []

    def expect(self, *calls):
        self._description.extend(calls)

    def connect(self, hostname, port):
        if port == 80:
            return gen.maybe_future(FakeIOStream(self, name="session"))
        elif port == 9000:
            return gen.maybe_future(FakeIOStream(self, name="push"))
        else:
            self.log.error("Unsupported port")
            assert False

    def get_connection_factory(self):
        return self

    def get_call_index_change(self):
        future = gen.Future()
        self._index_change_futures.append(future)
        return future

    def get_current_stream(self):
        if self._index < len(self._description):
            self.log.debug("Current index: %d", self._index)
            return self._description[self._index][0]
        else:
            return None

    def get_current_call(self):
        self.log.debug("Current index: %d", self._index)
        return self._description[self._index][1:]

    def move_to_next_call(self):
        self._index += 1
        self.log.info("Move to next call. Index: %d", self._index)
        for f in self._index_change_futures:
            f.set_result(None)
        del self._index_change_futures[:]


class FakeIOStream(object):
    log = logging.getLogger("FakeIOStream")

    IGNORE = object()

    def __init__(self, serialization, name):
        self._serializaton = serialization
        self._name = name

    def _compare_lists(self, expected, actual):
        assert len(expected) == len(actual)
        for i in xrange(len(expected)):
            if expected[i] != FakeIOStream.IGNORE:
                assert expected[i] == actual[i]

    def _compare_dicts(self, expected, actual):
        assert len(expected) == len(actual)
        for key, value in expected.iteritems():
            if value != FakeIOStream.IGNORE:
                assert key in actual
                assert value == actual[key]

    @gen.coroutine
    def method_missing(self, name, *args, **kwargs):
        self.log.info("%s called with %r and %r", name, args, kwargs)

        while True:
            if self._serializaton.get_current_stream() == self._name:
                try:
                    expected_name, expected_args, expected_kwargs, return_value = self._serializaton.get_current_call()
                    assert expected_name == name
                    self._compare_lists(expected_args, args)
                    self._compare_dicts(expected_kwargs, kwargs)
                    self._serializaton.move_to_next_call()
                except Exception:
                    self.log.error("Unhandled exception", exc_info=True)
                    raise

                if issubclass(type(return_value), Exception):
                    self.log.info("Raise exception: %r", return_value)
                    raise return_value
                else:
                    raise gen.Return(return_value)
            else:
                yield self._serializaton.get_call_index_change()

    def __getattr__(self, name):
        return functools.partial(self.method_missing, name)


def call(stream_name, name, return_value, *args, **kwargs):
    return (stream_name, name, args, kwargs, return_value)


class TestSessionStream(testing.AsyncTestCase):
    good_response = """HTTP/1.1 200 OK
Server: nginx/1.4.4
Date: Wed, 19 Mar 2014 11:09:54 GMT
Content-Type: text/plain
Transfer-Encoding: chunked
Connection: keep-alive
Vary: Accept-Encoding
Session: 00291e7c-eedf-42cd-99cc-f18331b9db77
Seqno: 3488
Lines: 218
PartOffset: 396
Topic: rt3.fol--other
Partition: 0\r\n\r\n"""

    session_id_missing_response = """HTTP/1.1 200 OK
Server: nginx/1.4.4
Date: Wed, 19 Mar 2014 11:09:54 GMT
Content-Type: text/plain
Transfer-Encoding: chunked
Connection: keep-alive
Vary: Accept-Encoding\r\n\r\n"""

    endpoint = (KAFKA_ENDPOINT, 80)

    def setUp(self):
        super(TestSessionStream, self).setUp()
        self._world_serialization = WorldSerialization()
        self.s = fennel.SessionStream(service_id=TEST_SERVICE_ID, source_id="", io_loop=self.io_loop, connection_factory=self._world_serialization)

    @testing.gen_test
    def test_basic(self):
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
        )
        result = yield self.s.connect(self.endpoint)
        assert result == "00291e7c-eedf-42cd-99cc-f18331b9db77"

    @testing.gen_test
    def test_session_id_missing(self):
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.session_id_missing_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
        )
        result = yield self.s.connect(self.endpoint)
        assert result == "00291e7c-eedf-42cd-99cc-f18331b9db77"

    @testing.gen_test
    def test_read_message(self):
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("session", "read_until", "4\r\n", "\r\n", max_bytes=FakeIOStream.IGNORE),
            call("session", "read_bytes", "ping\r\n", 6),
        )
        yield self.s.connect(self.endpoint)
        message = yield self.s.read_message()
        assert message.type == "ping"


class TestLogBroker(testing.AsyncTestCase):
    good_response = """HTTP/1.1 200 OK
Server: nginx/1.4.4
Date: Wed, 19 Mar 2014 11:09:54 GMT
Content-Type: text/plain
Transfer-Encoding: chunked
Connection: keep-alive
Vary: Accept-Encoding
Session: 00291e7c-eedf-42cd-99cc-f18331b9db77
Seqno: 0
Lines: 218
PartOffset: 396
Topic: rt3.fol--other
Partition: 0\r\n\r\n"""

    def setUp(self):
        super(TestLogBroker, self).setUp()
        self._world_serialization = WorldSerialization()

        self._service_id = "fenneltest"
        self._source_id = uuid.uuid4().hex

        self.logbroker = fennel.LogBroker(service_id=self._service_id, source_id=self._source_id, io_loop=self.io_loop, connection_factory=self._world_serialization)

    @testing.gen_test
    def test_save_one_chunk(self):
        ack_response = "chunk=0\toffset=0\tseqno=32\tpart_offset=111"
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", "{0}\r\n".format(hex(len(ack_response))[2:]), "\r\n", max_bytes=FakeIOStream.IGNORE),
            call("session", "read_bytes", "{0}\r\n".format(ack_response), len(ack_response) + 2),
        )
        yield self.logbroker.connect(KAFKA_ENDPOINT)
        seqno = yield self.logbroker.save_chunk(32, [{}])
        assert seqno == 32

    @testing.gen_test
    def test_save_one_big_chunk(self):
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
        )
        yield self.logbroker.connect(KAFKA_ENDPOINT)
        fennel.LogBroker.MAX_CHUNK_SIZE = 1000
        big_data = [{"key_{0}".format(i): "some_value"} for i in xrange(10**3)]
        with pytest.raises(fennel.ChunkTooBigError):
            yield self.logbroker.save_chunk(32, big_data)

    @testing.gen_test
    def test_save_two_ordered_chunks(self):
        seqnos = [ 32, 50 ]
        ack_response = ["chunk=0\toffset=0\tseqno={0}\tpart_offset=111".format(seqno) for seqno in seqnos]
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
        )
        for response in ack_response:
            self._world_serialization.expect(
                call("push", "write", None, FakeIOStream.IGNORE),
                call("session", "read_until", "{0}\r\n".format(self.get_hex_length(response)), "\r\n", max_bytes=FakeIOStream.IGNORE),
                call("session", "read_bytes", "{0}\r\n".format(response), len(response) + 2),
            )

        yield self.logbroker.connect(KAFKA_ENDPOINT)
        for expected_seqno in seqnos:
            seqno = yield self.logbroker.save_chunk(expected_seqno, [{}])
            assert expected_seqno == seqno

    @testing.gen_test
    def test_save_two_unordered_chunks(self):
        ack_response = [
            "chunk=0\toffset=0\tseqno=50\tpart_offset=111",
            "skip\tchunk=0\toffset=0\tseqno=32"
        ]
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
        )
        for response in ack_response:
            self._world_serialization.expect(
                call("push", "write", None, FakeIOStream.IGNORE),
                call("session", "read_until", "{0}\r\n".format(self.get_hex_length(response)), "\r\n", max_bytes=FakeIOStream.IGNORE),
                call("session", "read_bytes", "{0}\r\n".format(response), len(response) + 2),
            )

        yield self.logbroker.connect(KAFKA_ENDPOINT)

        seqno = yield self.logbroker.save_chunk(50, [{}])
        assert seqno == 50
        seqno = yield self.logbroker.save_chunk(32, [{}])
        assert seqno == 50

    @testing.gen_test
    def test_ack_dont_send_future_futures(self):
        ack_response = [
            "chunk=0\toffset=0\tseqno=32\tpart_offset=111",
        ]
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
        )
        for response in ack_response:
            self._world_serialization.expect(
                call("session", "read_until", "{0}\r\n".format(self.get_hex_length(response)), "\r\n", max_bytes=FakeIOStream.IGNORE),
                call("session", "read_bytes", "{0}\r\n".format(response), len(response) + 2),
            )

        yield self.logbroker.connect(KAFKA_ENDPOINT)

        f1 = self.logbroker.save_chunk(32, [{}])
        f2 = self.logbroker.save_chunk(50, [{}])
        seqno = yield f1
        assert seqno == 32
        assert not f2.done()

    @testing.gen_test
    def test_unsuccesfull_save_one_chunk(self):
        response = ""
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", "{0}\r\n".format(hex(len(response))[2:]), "\r\n", max_bytes=FakeIOStream.IGNORE),
            call("session", "read_until_close", None),
            call("session", "close", None),
        )
        yield self.logbroker.connect(KAFKA_ENDPOINT)
        with pytest.raises(fennel.SessionEndError):
            seqno = yield self.logbroker.save_chunk(32, [{}])

    @testing.gen_test
    def test_session_closed_save_one_chunk(self):
        response = ""
        self._world_serialization.expect(
            call("session", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", self.good_response, "\r\n\r\n", max_bytes=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("push", "read_until_close", None, streaming_callback=FakeIOStream.IGNORE),
            call("push", "write", None, FakeIOStream.IGNORE),
            call("session", "read_until", iostream.StreamClosedError(), "\r\n", max_bytes=FakeIOStream.IGNORE),
            call("session", "close", None),
        )
        yield self.logbroker.connect(KAFKA_ENDPOINT)
        with pytest.raises(iostream.StreamClosedError):
            seqno = yield self.logbroker.save_chunk(32, [{}])

    def get_hex_length(self, text):
        return hex(len(text))[2:]
