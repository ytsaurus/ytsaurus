#!/usr/bin/python

import yt.wrapper as yt

from tornado import ioloop
from tornado import iostream
from tornado import gen

import socket
import struct
import logging
import json
import datetime


DEFAULT_TABLE_NAME = "//sys/scheduler/event_log"
DEFAULT_KAFKA_ENDPOINT = ("kafka02gt.stat.yandex.net", 9000)
DEFAULT_CHUNK_SIZE = 4000
DEFAULT_SERVICE_ID = "yt"
DEFAULT_SOURCE_ID = "tramsmm43"

CHUNK_HEADER_FORMAT = "<QQQ"
CHUNK_HEADER_SIZE = struct.calcsize(CHUNK_HEADER_FORMAT)


class State(object):
    log = logging.getLogger("State")

    def __init__(self, event_log, chunk_size=DEFAULT_CHUNK_SIZE, **options):
        self.log_broker_ = None
        self.log_broker_options_ = options
        self.event_log_ = event_log
        self.chunk_size = chunk_size
        self.last_saved_seqno_ = 0
        self.last_seqno_ = 0
        self.acked_seqno_ = set()

    def start(self, io_loop=None, IOStreamClass=None):
        self.init_seqno()
        self.log_broker_ = LogBroker(self, io_loop, IOStreamClass, **self.log_broker_options_)
        self.log_broker_.start()
        self.maybe_save_another_chunk()

    def init_seqno(self):
        self.last_saved_seqno_ = self.from_line_index(self.event_log_.get_next_line_to_save())
        self.last_seqno_ = self.last_saved_seqno_
        self.log.info("Last saved seqno is %d", self.last_seqno_)

    def maybe_save_another_chunk(self):
        if self.last_saved_seqno_ - self.last_seqno_ < 1:
            self.log.debug("Schedule chunk save")
            seqno = self.last_saved_seqno_ + 1
            data = self.event_log_.get_data(self.to_line_index(seqno), self.chunk_size)
            self.log_broker_.save_chunk(seqno, data)
            self.last_saved_seqno_ = seqno

    def on_skip(self, seqno):
        if seqno > self.last_seqno_:
            last_seqno = seqno
            for i in self.acked_seqno_:
                if i > seqno:
                    if i == last_seqno + 1:
                        last_seqno += 1
                    else:
                        break
            self.update_last_seqno(last_seqno)

    def on_save_ack(self, seqno):
        self.acked_seqno_.add(seqno)
        last_seqno = self.last_seqno_
        for seqno in self.acked_seqno_:
            if seqno == last_seqno + 1:
                last_seqno += 1
            else:
                break
        if last_seqno > self.last_seqno_:
            self.update_last_seqno(last_seqno)

    def update_last_seqno(self, new_last_seqno):
        self.log.debug("Update last seqno: %d", new_last_seqno)

        self.event_log_.set_next_line_to_save(self.to_line_index(new_last_seqno))
        self.last_seqno_ = new_last_seqno
        for seqno in list(self.acked_seqno_):
            if seqno <= self.last_seqno_:
                self.acked_seqno_.remove(seqno)
        self.maybe_save_another_chunk()

    def to_line_index(self, reqno):
        return reqno * self.chunk_size

    def from_line_index(self, line_index):
        return line_index / self.chunk_size


class EventLog(object):
    def __init__(self, yt, table_name=None):
        self.yt = yt
        self.table_name_ = table_name or "//tmp/event_log"
        self.index_of_first_line_attr_ = "{0}/@index_of_first_line".format(self.table_name_)
        self.lines_to_save_attr_ = "{0}/@lines_to_save".format(self.table_name_)

    def get_data(self, begin, count):
        with self.yt.Transaction():
            lines_removed = int(self.yt.get(self.index_of_first_line_attr_))
            begin -= lines_removed
            assert begin >= 0
            return [json.loads(item) for item in self.yt.read_table("{0}[#{1}:#{2}]".format(
                self.table_name_,
                begin,
                begin + count), format="json")]

    def set_next_line_to_save(self, line_index):
        self.yt.set(self.lines_to_save_attr_, line_index)

    def get_next_line_to_save(self):
        return self.yt.get(self.lines_to_save_attr_)

    def init_if_not_initialized(self):
        with self.yt.Transaction():
            if not self.yt.exists(self.lines_to_save_attr_):
                self.yt.set(self.lines_to_save_attr_, 0)
            if not self.yt.exists(self.index_of_first_line_attr_):
                self.yt.set(self.index_of_first_line_attr_, 0)


def serialize_chunk(chunk_id, seqno, lines, data):
    serialized_data = struct.pack(CHUNK_HEADER_FORMAT, chunk_id, seqno, lines)
    for row in data:
        serialized_data += json.dumps(row)
    return serialized_data


def parse_chunk(serialized_data):
    serialized_data = serialized_data.strip()

    index = serialized_data.find("\r\n")
    assert index != -1
    index += len("\r\n")

    chunk_id, seqno, lines = struct.unpack(CHUNK_HEADER_FORMAT, serialized_data[index:index + CHUNK_HEADER_SIZE])
    index += CHUNK_HEADER_SIZE

    data = []
    decoder = json.JSONDecoder()
    while index < len(serialized_data):
        item, shift = decoder.raw_decode(serialized_data[index:])
        index += shift
        data.append(item)
    return data


class LogBroker(object):
    log = logging.getLogger("log_broker")

    def __init__(self, state, io_loop=None, IOStreamClass=None, endpoint=None, **options):
        self.endpoint_ = endpoint or DEFAULT_KAFKA_ENDPOINT
        self.state_ = state
        self.starting_ = False
        self.chunk_id_ = 0
        self.lines_ = 0
        self.session_ = None
        self.session_options_ = options
        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.iostream_ = None
        self.pending_data = []
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def start(self):
        if not self.starting_:
            self.starting_ = True
            self.log.info("Start a log broker")
            self.session_ = Session(self.state_, self, self.io_loop_, self.IOStreamClass, endpoint=self.endpoint_, **self.session_options_)
            self.session_.connect()

    def save_chunk(self, seqno, data):
        if self.iostream_ is not None:
            serialized_data = serialize_chunk(self.chunk_id_, seqno, self.lines_, data)
            self.chunk_id_ += 1
            self.lines_ += 1

            self.log.debug("Save chunk [%d]", seqno)
            data_to_write = "{size:X}\r\n{data}\r\n".format(size=len(serialized_data), data=serialized_data)
            self.iostream_.write(data_to_write)
        else:
            self.start()
            self.log.debug("Add %d chunk to pending queue", seqno)
            self.pending_data.append((seqno, data))

    def on_session_changed(self, id_):
        self.starting_ = False
        # close old iostream_
        self.chunk_id_ = 0
        self.lines_ = 0

        self.log.info("Create a push channel")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.iostream_ = self.IOStreamClass(s, io_loop=self.io_loop_)
        self.iostream_.set_close_callback(self.on_close)
        self.iostream_.connect(self.endpoint_, callback=self.on_connect)
        self.log.info("Send request")
        self.iostream_.write(
            "PUT /rt/store HTTP/1.1\r\n"
            "Host: 127.0.0.1\r\n"
            "Content-Type: text/plain\r\n"
            "Transfer-Encoding: chunked\r\n"
            "RTSTreamFormat: v2le\r\n"
            "Session: {session_id}\r\n"
            "\r\n".format(
                session_id=id_)
        )
        self.iostream_.read_until_close(self.on_response_end, self.on_response)
        for seqno, data in self.pending_data:
            self.save_chunk(seqno, data)
        self.pending_data = []

    def on_response(self, data):
        self.log.debug(data)

    def on_response_end(self, data):
        pass

    def on_connect(self):
        self.log.info("The push channel has been created")

    def on_close(self):
        self.log.info("The push channel has been closed")


class Session(object):
    log = logging.getLogger("session")

    def __init__(
            self,
            state,
            log_broker,
            io_loop=None,
            IOStreamClass=None,
            endpoint=None,
            service_id=None,
            source_id=None):
        self.endpoint_ = endpoint or DEFAULT_KAFKA_ENDPOINT
        self.service_id_ = service_id or DEFAULT_SERVICE_ID
        self.source_id_ = source_id or DEFAULT_SOURCE_ID
        self.id_ = None
        self.state_ = state
        self.log_broker_ = log_broker
        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.iostream_ = None
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def connect(self):
        assert self.iostream_ is None
        self.log.info("Connect to kafka")

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.iostream_ = self.IOStreamClass(s, io_loop=self.io_loop_)
        self.iostream_.set_close_callback(self.on_close)
        self.iostream_.connect(self.endpoint_, callback=self.on_connect)
        self.log.info("Send request")
        self.iostream_.write(
            "GET /rt/session?"
            "ident={ident}&"
            "sourceid={source_id} "
            "HTTP/1.1\r\n"
            "Host: kafka02gt.stat.yandex.net\r\n"
            "Accept: */*\r\n\r\n".format(
                ident="yt",
                source_id=self.source_id_)
        )

    @gen.coroutine
    def on_connect(self):
        metadata_raw = yield gen.Task(self.iostream_.read_until, "\r\n\r\n")

        self.log.debug("Parse response %s", metadata_raw)
        self.read_metadata(metadata_raw[:-4])

        while True:
            headers_raw = yield gen.Task(self.iostream_.read_until, "\r\n")
            data = yield gen.Task(self.iostream_.read_until, "\r\n")

            self.log.debug("Process status: %s", data)
            self.process_data(data)

    def read_metadata(self, data):
        for index, line in enumerate(data.split("\n")):
            if index > 0:
                key, value = line.split(":", 1)
                if key.strip() == "Session":
                    self.id_ = value.strip()
                    self.log.info("Session id: %s", self.id_)
                    self.log_broker_.on_session_changed(self.id_)

    def on_close(self):
        self.log.error("Connection is closed")
        self.iostream_ = None
        self.io_loop_.add_timeout(datetime.timedelta(seconds=1), self.connect)

    def process_data(self, data):
        if data.startswith("skip"):
            line = data[len("skip") + 1:]
            handler = self.state_.on_skip
        else:
            line = data
            handler = self.state_.on_save_ack

        attributes = self._parse(line)
        try:
            handler(attributes["seqno"])
        except KeyError:
            pass

    def _parse(self, line):
        attributes = {}
        records = line.split()
        for record in records:
            try:
                key, value = record.split("=", 1)
                value = int(value)
            except ValueError:
                pass
            else:
                attributes[key] = value
        return attributes


def main(table_name, proxy_path, service_id, source_id, chunk_size, **kwargs):
    io_loop = ioloop.IOLoop.instance()
    def stop():
        io_loop.stop()

    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    state = State(event_log=event_log, chunk_size=chunk_size, service_id=service_id, source_id=source_id)
    state.start()
    io_loop.start()


def init(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.init_if_not_initialized()


def run():
    from tornado import options

    import sys
    import os
    import atexit

    options.define("table_name",
        metavar="PATH",
        default=DEFAULT_TABLE_NAME,
        help="[yt] path to scheduler event log")
    options.define("proxy_path", metavar="URL", help="[yt] url to proxy")
    options.define("chunk_size", default=DEFAULT_CHUNK_SIZE, help="size of chunk in rows")

    options.define("service_id", default=DEFAULT_SERVICE_ID, help="[logbroker] service id")
    options.define("source_id", default=DEFAULT_SOURCE_ID, help="[logbroker] source id")

    options.define("init", default=False, help="init and exit")

    options.define("log_dir", metavar="PATH", default="/var/log/fennel", help="log directory")
    options.define("verbose", default=False, help="vervose mode")

    options.parse_command_line()

    logging.debug("Started")

    @atexit.register
    def log_exit():
        logging.debug("Exited")

    if options.options.init:
        func = init
    else:
        func = main

    func(**options.options.as_dict())


if __name__ == "__main__":
    run()
