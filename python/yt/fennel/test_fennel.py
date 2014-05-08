import fennel

from tornado import iostream
from tornado import ioloop

from yt import wrapper as yt

import pytest
import mock
import datetime
import unittest
import json


@pytest.fixture
def fake_state():
    state = fennel.State(event_log=mock.Mock(name="event_log"))
    state.log_broker_ = mock.Mock(name="log_broker")
    return state


def test_on_skip_new(fake_state):
    fake_state.on_skip(10);
    assert fake_state.event_log_.set_next_line_to_save.called
    assert fake_state.last_seqno_ == 10


def test_on_skip_old(fake_state):
    fake_state.last_seqno_ = 20
    fake_state.on_skip(10);
    assert not fake_state.event_log_.set_next_line_to_save.called
    assert fake_state.last_seqno_ == 20


def test_on_save_ack_next(fake_state):
    assert fake_state.last_seqno_ == 0
    fake_state.on_save_ack(1)
    assert fake_state.event_log_.set_next_line_to_save.called
    assert fake_state.last_seqno_ == 1


def test_on_save_ack_not_next(fake_state):
    assert fake_state.last_seqno_ == 0
    fake_state.on_save_ack(2)
    assert fake_state.last_seqno_ == 0
    assert not fake_state.event_log_.set_next_line_to_save.called


def test_on_save_ack_reordered(fake_state):
    assert fake_state.last_seqno_ == 0
    fake_state.on_save_ack(2)
    fake_state.on_save_ack(1)
    assert fake_state.event_log_.set_next_line_to_save.called
    assert fake_state.last_seqno_ == 2


def test_on_skip_removes_old_acks(fake_state):
    fake_state.on_save_ack(2)
    fake_state.on_save_ack(11)
    fake_state.on_skip(10)
    assert fake_state.event_log_.set_next_line_to_save.called
    assert fake_state.last_seqno_ == 11


def test_do_not_save(fake_state):
    fake_state.last_saved_seqno_ = 1
    fake_state.maybe_save_another_chunk()
    assert fake_state.last_saved_seqno_ == 1


def test_save(fake_state):
    fake_state.last_saved_seqno_ = 0
    fake_state.maybe_save_another_chunk()
    assert fake_state.last_saved_seqno_ == 1


def test_event_log_get_data():
    fake_yt = mock.Mock(name="yt")
    fake_yt.get = mock.Mock(return_value=200)
    fake_yt.read_table = mock.Mock(return_value=[json.dumps("1")])
    fake_yt.Transaction = mock.MagicMock()
    event_log = fennel.EventLog(fake_yt, table_name="//tmp/event_log")
    event_log.get_data(200, 1000)
    fake_yt.read_table.assert_called_with("//tmp/event_log[#0:#1000]", format="json")


@pytest.fixture
def fake_session():
    return fennel.Session(
        mock.Mock(spec=fennel.State, name="state"),
        mock.Mock(spec=fennel.LogBroker, name="logbroker"),
        mock.Mock(name="ioloop"),
        mock.Mock(name="IOStreamClass"))


def test_session_process_data_skip(fake_session):
    fake_session.process_data("skip    chunk=16        offset=256      seqno=256")
    assert not fake_session.state_.on_save_ack.called
    fake_session.state_.on_skip.assert_called_with(256)


def test_session_process_data_ack(fake_session):
    fake_session.process_data("chunk=19        offset=304      seqno=304       part_offset=111")
    assert not fake_session.state_.on_skip.called
    fake_session.state_.on_save_ack.assert_called_with(304)


def test_session_process_data_no_seqno(fake_session):
    fake_session.process_data("offset=304      part_offset=111")
    assert not fake_session.state_.on_save_ack.called
    assert not fake_session.state_.on_save_ack.called


def test_session_process_data_seqno_is_nan(fake_session):
    fake_session.process_data("offset=304      seqno=xxx      part_offset=111")
    assert not fake_session.state_.on_save_ack.called
    assert not fake_session.state_.on_save_ack.called

def test_session_process_data_bad_record(fake_session):
    fake_session.process_data("offset=304      seqno      part_offset=111")
    assert not fake_session.state_.on_save_ack.called
    assert not fake_session.state_.on_save_ack.called


def test_session_read_good_id(fake_session):
    data = """HTTP/1.1 200 OK
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
Partition: 0"""
    fake_session.read_metadata(data)
    assert fake_session.id_ == "00291e7c-eedf-42cd-99cc-f18331b9db77"


class StreamToKafka(object):
    def __init__(self, session_data, store_data, io_loop, stream_holder=None, counters=None):
        self.session_data_ = session_data
        self.store_data_ = store_data
        self.data_ = None
        self.index_ = 0
        self.io_loop_ = io_loop
        self.output_data_ = []
        self.close_callback_ = None
        self.stream_holder_ = stream_holder
        self.counters_ = counters or 0

    def write(self, data):
        if len(self.output_data_) == 0:
            name = None
            if data.startswith("GET /rt/session"):
                name = "session"
                self.data_ = self.session_data_
            else:
                name = "store"
                self.data_ = self.store_data_

            if name is not None and self.stream_holder_ is not None:
                self.stream_holder_[name] = self

        self.output_data_.append(data)

    def connect(self, endpoint, callback):
        if self.counters_.fail_connections_ > 0:
            self.counters_.fail_connections_ -= 1
            if self.close_callback_:
                self.io_loop_.add_callback(self.close_callback_)
        else:
            self.io_loop_.add_callback(callback)

    def read_until(self, delimiter, callback):
        index = self.data_.find(delimiter, self.index_)
        if index != -1:
            index += len(delimiter)
            self.io_loop_.add_callback(callback, self.data_[self.index_:index])
            self.index_ = index
        else:
            self.io_loop_.stop()

    def read_until_close(self, callback, streaming_callback):
        pass

    def set_close_callback(self, callback):
        self.close_callback_ = callback


session_data = """HTTP/1.1 200 OK
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
Partition: 0\r\n\r\n5
ping
"""


class IOLoopedTestCase(unittest.TestCase):
    def setUp(self):
        self.stream_holder = {}
        self.force_stop = False
        self.io_loop = ioloop.IOLoop()
        self.io_loop.add_timeout(datetime.timedelta(seconds=1), self.stop)
        self.fail_connections_ = 0

    def tearDown(self):
        pass

    def test_empty(self):
        pass

    def start(self):
        self.io_loop.start()

    def stop(self):
        self.force_stop = True
        self.io_loop.stop()

    def stream_factory(self, s, io_loop):
        return StreamToKafka(
            session_data,
            "",
            io_loop,
            stream_holder=self.stream_holder,
            counters=self)


class TestSession(IOLoopedTestCase):
    def test_connect(self):
        s = fennel.Session(
            mock.Mock(name="state"),
            mock.Mock(name="log_broker"),
            self.io_loop,
            self.stream_factory)
        s.connect()
        self.start()
        assert s.id_ is not None
        assert not self.force_stop


class TestSessionReconanect(IOLoopedTestCase):
    def test_basic(self):
        self.fail_connections_ = 1
        s = fennel.Session(
            mock.Mock(name="state"),
            mock.Mock(name="log_broker"),
            self.io_loop,
            self.stream_factory)
        s.connect()
        self.start()
        assert s.id_ is not None
        assert not self.force_stop


class TestSaveChunk(IOLoopedTestCase):
    def test_basic(self):
        l = fennel.LogBroker(
            mock.Mock(name="state"),
            io_loop=self.io_loop,
            IOStreamClass = self.stream_factory)
        l.save_chunk(0, [{"key0":"value0"}])
        l.save_chunk(1, [{"key1":"value1"}])
        l.save_chunk(2, [{"key2":"value2"}, {"key3":"value3"}])
        self.start()

        assert "session" in self.stream_holder
        assert "store" in self.stream_holder

        chunks = [fennel.parse_chunk(x) for x in self.stream_holder["store"].output_data_[1:]]
        assert len(chunks) == 3
        assert chunks[0][0]["key0"] == "value0"
        assert chunks[1][0]["key1"] == "value1"
        assert chunks[2][1]["key3"] == "value3"


def test_session_integration():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    io_loop = ioloop.IOLoop()
    def stop():
        io_loop.stop()

    io_loop.add_timeout(datetime.timedelta(seconds=1), stop)
    s = fennel.Session(mock.Mock(), mock.Mock(), io_loop, iostream.IOStream)
    s.connect()
    io_loop.start()
    assert s.id_ is not None
