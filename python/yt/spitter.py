#!/usr/bin/python

import yt.wrapper as yt

from tornado import ioloop
from tornado import iostream
from tornado import gen

import socket
import struct
import logging
import json


class State(object):
    def __init__(self, log_broker, event_log):
        self.log_broker_ = log_broker
        self.event_log_ = event_log
        self.chunk_size = 1000
        self.last_saved_seqno_ = 0
        self.last_seqno_ = 0
        self.acked_seqno_ = set()

    def maybe_save_another_chunk(self):
        if self.last_saved_seqno_ - self.last_seqno_ < 1:
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
        self.event_log_.set_next_line_to_save(self.to_line_index(new_last_seqno))
        self.last_seqno_ = new_last_seqno
        for seqno in list(self.acked_seqno_):
            if seqno <= self.last_seqno_:
                self.acked_seqno_.remove(seqno)
        self.maybe_save_another_chunk()

    def to_line_index(self, reqno):
        return reqno * self.chunk_size


class EventLog(object):
    def __init__(self, yt):
        self.yt = yt
        self.table_name_ = "//sys/scheduler/event_log"

    def get_data(self, begin, count):
        with self.yt.Transaction():
            lines_removed = int(self.yt.get("{0}/@index_of_first_line".format(self.table_name_)))
            begin -= lines_removed
            return self.yt.read("{0}[#{1}:#{2}]".format(
                self.table_name_,
                begin,
                begin + count))

    def set_next_line_to_save(self, line_index):
        self.yt.set("{0}/@lines_to_save", line_index - lines_removed)


def serialize_chunk(seqno, data):
    serialized_data = struct.pack("<QQQ", 0, seqno, 0)
    for row in data:
        serialized_data += json.dumps(row)
    return serialized_data


def parse_chunk(serialized_data):
    serialized_data = serialized_data.strip()

    index = serialized_data.find("\r\n")
    assert index != -1
    index += len("\r\n")

    _1, seqno, _3 = struct.unpack("<QQQ", serialized_data[index:index + 3*8])
    index += 3*8

    data = []
    decoder = json.JSONDecoder()
    while index < len(serialized_data):
        item, shift = decoder.raw_decode(serialized_data[index:])
        index += shift
        data.append(item)
    return data


class LogBroker(object):
    log = logging.getLogger("log_broker")

    def __init__(self, state, io_loop=None, IOStreamClass=None):
        self.endpoint_ = ("kafka02gt.stat.yandex.net", 9000)
#        self.endpoint_ = ("localhost", 9000)
        self.state_ = state
        self.starting_ = False
        self.session_ = None
        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.iostream_ = None
        self.pending_data = []
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def start(self):
        if not self.starting_:
            self.starting_ = True
            self.log.info("Start a log broker")
            self.session_ = Session(self.state_, self, self.io_loop_, self.IOStreamClass)
            self.session_.connect()

    def save_chunk(self, seqno, data):
        if self.iostream_ is not None:
            serialized_data = serialize_chunk(seqno, data)

            self.log.debug("Save chunk: %s", serialized_data)
            data_to_write = "{size:X}\r\n{data}\r\n".format(size=len(serialized_data), data=serialized_data)
            self.iostream_.write(data_to_write)
        else:
            self.start()
            self.pending_data.append((seqno, data))

    def on_session_changed(self, id_):
        self.starting_ = False
        # close old iostream_
        self.log.info("Create a push channel")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.iostream_ = self.IOStreamClass(s, io_loop=self.io_loop_)
        self.iostream_.set_close_callback(self.on_close)
        self.iostream_.connect(self.endpoint_, callback=self.on_connect)
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

    def __init__(self, state, log_broker, io_loop=None, IOStreamClass=None):
        self.endpoint_ = ("kafka02gt.stat.yandex.net", 9000)
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
                source_id="tramsmm42")
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
        pass

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


def main():
    pass


if __name__ == "__main__":
    main()
