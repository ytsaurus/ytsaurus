import spitter

from tornado import iostream
from tornado import ioloop

import pytest
import mock
import datetime


@pytest.fixture
def fake_state():
    return spitter.State(mock.Mock(name="log_broker"), mock.Mock(name="event_log"))


def test_on_skip_new(fake_state):
    fake_state.on_skip(10);
    assert fake_state.event_log_.set_next_line_to_save.called


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
    event_log = spitter.EventLog(mock.Mock(name="yt"))
    event_log.yt.get = mock.Mock(return_value = 200)
    event_log.yt.Transaction = mock.MagicMock()
    event_log.get_data(200, 1000)
    event_log.yt.read.assert_called_with("//sys/scheduler/event_log[#0:#1000]")


@pytest.fixture
def fake_session():
    return spitter.Session(
        mock.Mock(spec=spitter.State, name="state"),
        mock.Mock(spec=spitter.LogBroker, name="logbroker"),
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
    def __init__(self, session_data, store_data, io_loop, stream_holder=None):
        self.session_data_ = session_data
        self.store_data_ = store_data
        self.data_ = None
        self.index_ = 0
        self.io_loop_ = io_loop
        self.output_data_ = []
        self.stream_holder_ = stream_holder

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
        pass


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

def test_session_connect():
    failed = False
    io_loop = ioloop.IOLoop()
    def stop():
        failed = True
        io_loop.stop()

    io_loop.add_timeout(datetime.timedelta(seconds=1), stop)

    def stream_factory(s, io_loop):
        return StreamToKafka(session_data, "", io_loop)

    s = spitter.Session(
        mock.Mock(name="state"),
        mock.Mock(name="log_broker"),
        io_loop,
        stream_factory)
    s.connect()
    io_loop.start()
    assert s.id_ is not None
    assert not failed


def test_session_integration():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    io_loop = ioloop.IOLoop()
    def stop():
        io_loop.stop()

    io_loop.add_timeout(datetime.timedelta(seconds=1), stop)
    s = spitter.Session(mock.Mock(), mock.Mock(), io_loop, iostream.IOStream)
    s.connect()
    io_loop.start()
    assert s.id_ is not None


def test_save_chunk():
    stream_holder = {}
    io_loop = ioloop.IOLoop.instance()
    def stop():
        io_loop.stop()

    io_loop.add_timeout(datetime.timedelta(seconds=2), stop)
    def stream_factory(s, io_loop):
        return StreamToKafka(session_data, "", io_loop, stream_holder)

    l = spitter.LogBroker(
        mock.Mock(name="state"),
        io_loop=io_loop,
        IOStreamClass = stream_factory)
    l.save_chunk(0, [{"key0":"value0"}])
    l.save_chunk(1, [{"key1":"value1"}])
    l.save_chunk(2, [{"key2":"value2"}, {"key3":"value3"}])
    io_loop.start()

    assert "session" in stream_holder
    assert "store" in stream_holder

    chunks = [spitter.parse_chunk(x) for x in stream_holder["store"].output_data_[1:]]
    assert len(chunks) == 3
    assert chunks[0][0]["key0"] == "value0"
    assert chunks[1][0]["key1"] == "value1"
    assert chunks[2][1]["key3"] == "value3"


def test_integration():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    io_loop = ioloop.IOLoop.instance()
    def stop():
        io_loop.stop()

    io_loop.add_timeout(datetime.timedelta(seconds=4), stop)
    broker = spitter.LogBroker(mock.Mock(), io_loop, iostream.IOStream)
    broker.save_chunk(0, [{"key":"value"}])
    broker.save_chunk(1, [{"key":"value"}])
    broker.save_chunk(2, [{"key":"value"}])
    broker.save_chunk(3, [{"key":"value"}])
    io_loop.start()
