import fennel

from tornado import testing
from tornado import gen

import pytest

import uuid
import sys

pytestmark = pytest.mark.skipif("sys.version_info < (2,7)", reason="requires python2.7")

KAFKA_ENDPOINT = "kafka17h.stat.yandex.net"
TEST_SERVICE_ID = "fenneltest"


class TestLogBrokerIntegration(testing.AsyncTestCase):
    def setUp(self):
        super(TestLogBrokerIntegration, self).setUp()
        self.l = None
        self._init_exception_info = None
        self._inited = False
        self.init()
        self.wait()
        if self._init_exception_info:
            raise sys._init_exception_info

    def tearDown(self):
        self.l.stop()
        super(TestLogBrokerIntegration, self).tearDown()

    @gen.coroutine
    def init(self):
        try:
            self.source_id = uuid.uuid4().hex

            self.l = fennel.LogBroker(service_id=TEST_SERVICE_ID, source_id=self.source_id, io_loop=self.io_loop)
            seqno = yield self.l.connect(KAFKA_ENDPOINT)
            assert seqno == 0
        except BaseException as e:
            self._init_exception_info = sys.exc_info()
        finally:
            self.stop()

    @testing.gen_test
    def test_save_one_chunk(self):
        seqno = yield self.l.save_chunk(1, [{}])
        assert seqno == 1

    @testing.gen_test
    def test_save_two_concurrent_chunks(self):
        f1 = self.l.save_chunk(1, [{"key": i} for i in xrange(10**3)])
        f2 = self.l.save_chunk(2, [{}])
        seqnos = yield [f1, f2]
        assert  seqnos[0] >= 1
        assert  seqnos[1] == 2

    @testing.gen_test
    def test_save_chunks_unordered(self):
        seqno = yield self.l.save_chunk(20, [{}])
        assert seqno == 20
        seqno = yield self.l.save_chunk(10, [{}])
        assert seqno == 20


class TestSessionStreamIntegraion(testing.AsyncTestCase):
    @testing.gen_test
    def test_connect(self):
        source_id = uuid.uuid4().hex
        s = fennel.SessionStream(service_id=TEST_SERVICE_ID, source_id=source_id, io_loop=self.io_loop)
        session_id = yield s.connect((KAFKA_ENDPOINT, 80))
        assert session_id is not None
        s.get_attribute("seqno")

    @testing.gen_test
    def test_get_ping(self):
        source_id = uuid.uuid4().hex
        s = fennel.SessionStream(service_id=TEST_SERVICE_ID, source_id=source_id, io_loop=self.io_loop)
        session_id = yield s.connect((KAFKA_ENDPOINT, 80))
        assert session_id is not None
        message = yield s.read_message()
        assert message.type == "ping"


class TestSessionPushStreamIntegration(testing.AsyncTestCase):
    def setUp(self):
        super(TestSessionPushStreamIntegration, self).setUp()
        self._session_id = None
        self._init_exception_info = None
        self.init()
        self.wait()
        if self._init_exception_info:
            raise self._init_exception_info

    @gen.coroutine
    def init(self):
        try:
            self.source_id = uuid.uuid4().hex

            self.s = fennel.SessionStream(service_id=TEST_SERVICE_ID, source_id=self.source_id, io_loop=self.io_loop)
            self._session_id = yield self.s.connect((KAFKA_ENDPOINT, 80))
            assert self._session_id is not None

            self.p = fennel.PushStream(io_loop=self.io_loop)
            yield self.p.connect((KAFKA_ENDPOINT, 9000), session_id=self._session_id)
        except BaseException as e:
            self._init_exception_info = sys.exc_info()
        finally:
            self.stop()

    def write_chunk(self, seqno):
        data = [{}]
        serialized_data = fennel.serialize_chunk(0, seqno, 0, data)
        return self.p.write_chunk(serialized_data)

    @gen.coroutine
    def get_skip_message(self):
        message = None
        while message is None or message.type == "ping":
            message = yield self.s.read_message()
        assert message.type == "skip"
        raise gen.Return(message)

    @gen.coroutine
    def get_ack_message(self):
        message = None
        while message is None or message.type == "ping":
            message = yield self.s.read_message()
        assert message.type == "ack"
        raise gen.Return(message)

    @testing.gen_test
    def test_basic(self):
        yield self.write_chunk(1)

        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 1

    @testing.gen_test
    def test_two_consecutive(self):
        yield self.write_chunk(1)
        yield self.write_chunk(2)

        acked_seqno = 0
        while acked_seqno < 2:
            message = None
            while message is None or message.type == "ping":
                message = yield self.s.read_message()
            assert message.type == "ack"
            acked_seqno = message.attributes["seqno"]
        assert acked_seqno == 2

    @testing.gen_test
    def test_two_non_consecutive(self):
        yield self.write_chunk(10)
        yield self.write_chunk(20)

        acked_seqno = 0
        while acked_seqno < 20:
            message = None
            while message is None or message.type == "ping":
                message = yield self.s.read_message()
            assert message.type == "ack"
            acked_seqno = message.attributes["seqno"]
        assert acked_seqno == 20

    @testing.gen_test
    def test_two_unordered(self):
        yield self.write_chunk(20)

        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 20

        yield self.write_chunk(10)

        message = None
        while message is None or message.type == "ping":
            message = yield self.s.read_message()
        assert message.type == "skip"
        assert message.attributes["seqno"] == 10

    @testing.gen_test
    def test_two_equal(self):
        yield self.write_chunk(20)
        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 20

        yield self.write_chunk(20)
        message = yield self.get_skip_message()
        assert message.attributes["seqno"] == 20

    @testing.gen_test
    def test_session_seqno(self):
        yield self.write_chunk(20)
        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 20

        self.p.stop()
        self.s.stop()

        self.s = fennel.SessionStream(service_id=TEST_SERVICE_ID, source_id=self.source_id, io_loop=self.io_loop)
        session_id = yield self.s.connect((KAFKA_ENDPOINT, 80))
        assert session_id is not None

        self.p = fennel.PushStream(io_loop=self.io_loop)
        yield self.p.connect((KAFKA_ENDPOINT, 9000), session_id=session_id)

        assert int(self.s.get_attribute("seqno")) == 20

    @testing.gen_test
    def test_two_parallel_sessions(self):
        yield self.write_chunk(20)
        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 20

        s = fennel.SessionStream(service_id=TEST_SERVICE_ID, source_id=self.source_id, io_loop=self.io_loop)
        session_id = yield s.connect((KAFKA_ENDPOINT, 80))
        assert session_id is not None
        assert session_id != self._session_id
        assert int(s.get_attribute("seqno")) == 20

        yield self.write_chunk(40)
        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 40

    @testing.gen_test
    def test_save_empty_chunk(self):
        data = []
        serialized_data = fennel.serialize_chunk(0, 42, 0, data)
        self.p.write_chunk(serialized_data)

        message = yield self.get_ack_message()
        assert message.attributes["seqno"] == 42

