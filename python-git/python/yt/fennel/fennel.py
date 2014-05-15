#!/usr/bin/python

import yt.wrapper as yt

from tornado import ioloop
from tornado import iostream
from tornado import gen
from tornado import options

import sys
import os
import atexit
import socket
import struct
import logging
import json
import datetime
import zlib


DEFAULT_TABLE_NAME = "//sys/scheduler/event_log"
DEFAULT_KAFKA_ENDPOINT = ("kafka02gt.stat.yandex.net", 9000)
DEFAULT_CHUNK_SIZE = 4000
DEFAULT_ACK_QUEUE_LENGTH = 1
DEFAULT_SERVICE_ID = "yt"
DEFAULT_SOURCE_ID = "tramsmm43"

CHUNK_HEADER_FORMAT = "<QQQ"
CHUNK_HEADER_SIZE = struct.calcsize(CHUNK_HEADER_FORMAT)


class State(object):
    log = logging.getLogger("State")

    def __init__(self,
                 event_log,
                 io_loop=None,
                 chunk_size=DEFAULT_CHUNK_SIZE,
                 ack_queue_length=DEFAULT_ACK_QUEUE_LENGTH,
                 IOStreamClass=None,
                 **options):
        self.chunk_size_ = chunk_size
        self.ack_queue_length_ = ack_queue_length
        self.last_saved_seqno_ = 0
        self.last_seqno_ = 0
        self.acked_seqno_ = set()

        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.event_log_ = event_log
        self.log_broker_ = LogBroker(self, self.io_loop_, IOStreamClass, **options)

        self.save_chunk_handle_ = None
        self.update_state_handle_ = None

    def start(self):
        self._initialize()
        self.log_broker_.start()

    def abort(self):
        self.log_broker.abort()

    def _initialize(self):
        self.last_saved_seqno_ = self._from_line_index(self.event_log_.get_next_line_to_save())
        self.last_seqno_ = self.last_saved_seqno_
        self.log.info("Last acked seqno is %d", self.last_seqno_)

    def maybe_save_another_chunk(self):
        if self.save_chunk_handle_ is not None:
            # wait for callback
            return
        if self.last_saved_seqno_ - self.last_seqno_ < self.ack_queue_length_:
            self._save_chunk()

    def _save_chunk(self):
        self.log.debug("Schedule chunk save")
        self.save_chunk_handle_ = None
        try:
            seqno = self.last_saved_seqno_ + 1
            data = self.event_log_.get_data(self._to_line_index(seqno), self.chunk_size_)
            self.log_broker_.save_chunk(seqno, data)
            self.last_saved_seqno_ = seqno
        except yt.YtError:
            self.log.error("Unable to schedule chunk save", exc_info=True)
            self.save_chunk_handle_ = self.io_loop_.add_timeout(datetime.timedelta(seconds=1), self._save_chunk)
        except EventLog.NotEnoughDataError:
            self.log.warning("Unable to get {0} rows from event log".format(self.chunk_size_), exc_info=True)
            self.io_loop_.add_timeout(datetime.timedelta(seconds=120), self.maybe_save_another_chunk)
        else:
            self.io_loop_.add_callback(self.maybe_save_another_chunk)

    def on_session_changed(self):
        self.last_saved_seqno_ = self.last_seqno_
        self.log.info("Last acked seqno is %d", self.last_seqno_)
        self.maybe_save_another_chunk()

    def on_skip(self, seqno):
        self.log.debug("Skip seqno=%d", seqno)
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
        self.log.debug("Ack seqno=%d", seqno)
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

        self.last_seqno_ = new_last_seqno
        self.log.info("Last acked seqno is %d", self.last_seqno_)
        for seqno in list(self.acked_seqno_):
            if seqno <= self.last_seqno_:
                self.acked_seqno_.remove(seqno)

        if self.update_state_handle_ is None:
            self.update_state_handle_ = self.io_loop_.add_timeout(datetime.timedelta(seconds=5), self._update_state)

        self.maybe_save_another_chunk()

    def _update_state(self):
        self.log.debug("Update state. Last acked seqno: %d", self.last_seqno_)
        self.update_state_handle_ = None
        try:
            self.event_log_.set_next_line_to_save(self._to_line_index(self.last_seqno_))
        except yt.YtError:
            self.log.error("Unable to update next line to save", exc_info=True)
            self.update_state_handle_ = self.io_loop_.add_timeout(datetime.timedelta(seconds=1), self._update_state)

    def _to_line_index(self, reqno):
        return reqno * self.chunk_size_

    def _from_line_index(self, line_index):
        return line_index / self.chunk_size_


class EventLog(object):
    class NotEnoughDataError(RuntimeError):
        pass

    def __init__(self, yt, table_name=None):
        self.yt = yt
        self.table_name_ = table_name or "//tmp/event_log"
        self.index_of_first_line_attr_ = "{0}/@index_of_first_line".format(self.table_name_)
        self.lines_to_save_attr_ = "{0}/@lines_to_save".format(self.table_name_)

    def get_data(self, begin, count):
        result = None
        with self.yt.Transaction():
            lines_removed = int(self.yt.get(self.index_of_first_line_attr_))
            begin -= lines_removed
            assert begin >= 0
            result = [item for item in self.yt.read_table(yt.TablePath(
                self.table_name_,
                start_index=begin,
                end_index=begin + count), format="json", raw=False)]
        if len(result) != count:
            raise EventLog.NotEnoughDataError("Not enough data. Got only {0} rows".format(len(result)))
        return result

    def truncate(self, count):
        with self.yt.Transaction():
            index_of_first_line = int(self.yt.get(self.index_of_first_line_attr_))
            index_of_first_line += count
            self.yt.set(self.index_of_first_line_attr_, index_of_first_line)
            self.yt.run_erase(yt.TablePath(
                self.table_name_,
                start_index=0,
                end_index=count))

    def set_next_line_to_save(self, line_index):
        self.yt.set(self.lines_to_save_attr_, line_index)

    def get_next_line_to_save(self):
        return self.yt.get(self.lines_to_save_attr_)

    def initialize(self):
        with self.yt.Transaction():
            if not self.yt.exists(self.lines_to_save_attr_):
                self.yt.set(self.lines_to_save_attr_, 0)
            if not self.yt.exists(self.index_of_first_line_attr_):
                self.yt.set(self.index_of_first_line_attr_, 0)


def serialize_chunk(chunk_id, seqno, lines, data):
    serialized_data = struct.pack(CHUNK_HEADER_FORMAT, chunk_id, seqno, lines)
    serialized_data += zlib.compress("".join([json.dumps(row) for row in data]))
    return serialized_data


def parse_chunk(serialized_data):
    serialized_data = serialized_data.strip()

    index = serialized_data.find("\r\n")
    assert index != -1
    index += len("\r\n")

    chunk_id, seqno, lines = struct.unpack(CHUNK_HEADER_FORMAT, serialized_data[index:index + CHUNK_HEADER_SIZE])
    index += CHUNK_HEADER_SIZE

    decompressed_data = zlib.decompress(serialized_data[index:])
    i = 0

    data = []
    decoder = json.JSONDecoder()
    while i < len(decompressed_data):
        item, shift = decoder.raw_decode(decompressed_data[i:])
        i += shift
        data.append(item)
    return data


class LogBroker(object):
    log = logging.getLogger("log_broker")

    def __init__(self, state, io_loop=None, IOStreamClass=None, endpoint=None, **options):
        self.endpoint_ = endpoint or DEFAULT_KAFKA_ENDPOINT
        self.host_ = self.endpoint_[0]
        self.state_ = state
        self.starting_ = False
        self.chunk_id_ = 0
        self.lines_ = 0
        self.push_channel_ = None
        self.session_ = None
        self.session_options_ = options
        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def start(self):
        if not self.starting_:
            self.starting_ = True
            self.log.info("Start a log broker")
            self.session_ = Session(self.state_, self, self.io_loop_, self.IOStreamClass, endpoint=self.endpoint_, **self.session_options_)
            self.session_.connect()

    def abort(self):
        self.push_channel_.abort()
        self.session_.abort()

    def save_chunk(self, seqno, data):
        if self.push_channel_ is not None:
            serialized_data = serialize_chunk(self.chunk_id_, seqno, self.lines_, data)
            self.chunk_id_ += 1
            self.lines_ += 1

            self.log.debug("Save chunk [%d]", seqno)
            data_to_write = "{size:X}\r\n{data}\r\n".format(size=len(serialized_data), data=serialized_data)
            self.push_channel_.write(data_to_write)
        else:
            assert False

    def on_session_changed(self, id_):
        self.starting_ = False
        if self.push_channel_ is not None:
            self.push_channel_.abort()

        self.chunk_id_ = 0
        self.lines_ = 0

        self.push_channel_ = PushChannel(self.state_, id_,
            io_loop=self.io_loop_,
            IOStreamClass=self.IOStreamClass,
            endpoint=self.endpoint_)
        self.push_channel_.connect()


class PushChannel(object):
    log = logging.getLogger("push_channel")

    def __init__(self, state, session_id, io_loop=None, IOStreamClass=None, endpoint=None):
        self.state_ = state
        self.session_id_ = session_id
        self.aborted_ = False

        self.endpoint_ = endpoint or DEFAULT_KAFKA_ENDPOINT
        self.host_ = self.endpoint_[0]

        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.iostream_ = None
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def connect(self):
        self.log.info("Create a push channel")
        if self.aborted_:
            self.log.error("Unable to connect: channel is aborted")
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self.iostream_ = self.IOStreamClass(s, io_loop=self.io_loop_)
        self.iostream_.set_close_callback(self.on_close)
        self.iostream_.connect(self.endpoint_, callback=self.on_connect)
        self.log.info("Send request")
        self.iostream_.write(
            "PUT /rt/store HTTP/1.1\r\n"
            "Host: {host}\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Encoding: gzip\r\n"
            "Transfer-Encoding: chunked\r\n"
            "RTSTreamFormat: v2le\r\n"
            "Session: {session_id}\r\n"
            "\r\n".format(
                host=self.host_,
                session_id=self.session_id_)
        )

    def write(self, data):
        if self.aborted_:
            self.log.error("Unable to write: channel is aborted")
            return
        self.iostream_.write(data)

    def abort(self):
        self.log.info("Abort the push channel")
        self.aborted_ = True
        if self.iostream_ is not None:
            self.iostream_.close()

    def on_connect(self):
        self.log.info("The push channel has been created")
        self.state_.on_session_changed()
        self.iostream_.read_until_close(self.on_response_end, self.on_response)

    def on_response(self, data):
        self.log.debug(data)

    def on_response_end(self, data):
        pass

    def on_close(self):
        self.log.info("The push channel has been closed")
        self.iostream_ = None
        if not self.aborted_:
            self.io_loop_.add_timeout(datetime.timedelta(seconds=1), self.connect)


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
        self.host_ = self.endpoint_[0]
        self.service_id_ = service_id or DEFAULT_SERVICE_ID
        self.source_id_ = source_id or DEFAULT_SOURCE_ID
        self.id_ = None
        self.aborted_ = False
        self.state_ = state
        self.log_broker_ = log_broker
        self.io_loop_ = io_loop or ioloop.IOLoop.instance()
        self.iostream_ = None
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def connect(self):
        assert self.iostream_ is None
        if self.aborted_:
            return
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
            "Host: {host}\r\n"
            "Accept: */*\r\n\r\n".format(
                ident="yt",
                source_id=self.source_id_,
                host=self.host_)
        )

    @gen.coroutine
    def on_connect(self):
        self.log.info("The session channel has been created")
        metadata_raw = yield gen.Task(self.iostream_.read_until, "\r\n\r\n")

        self.log.debug("Parse response %s", metadata_raw)
        result = self.read_metadata(metadata_raw[:-4])
        if not result:
            self.log.error("Unable to find Session header in the response")
            self.iostream_.close()
            return

        try:
            while True:
                headers_raw = yield gen.Task(self.iostream_.read_until, "\r\n")
                try:
                    body_size = int(headers_raw, 16)
                except ValueError:
                    self.log.error("Bad HTTP chunk header format")
                    self.iostream_.close()
                    return
                if body_size == 0:
                    self.log.error("HTTP response is finished")
                    self.iostream_.close()
                    return
                data = yield gen.Task(self.iostream_.read_bytes, body_size + 2)

                self.log.debug("Process status: %s", data.strip())
                self.process_data(data.strip())
        except Exception:
            self.log.error("Unhandled exception. Close the push channel", exc_info=True)
            self.iostream_.close()
            raise

    def read_metadata(self, data):
        for index, line in enumerate(data.split("\n")):
            if index > 0:
                key, value = line.split(":", 1)
                if key.strip() == "Session":
                    self.id_ = value.strip()
                    self.log.info("Session id: %s", self.id_)
                    self.log_broker_.on_session_changed(self.id_)
                    return True
        return False

    def abort(self):
        self.log.info("Abort the session channel")
        self.aborted_ = True
        if self.iostream is not None:
            self.iostream_.close()

    def on_close(self):
        self.log.error("The session channel has been closed")
        self.iostream_ = None
        if not self.aborted_:
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


def main(table_name, proxy_path, service_id, source_id, chunk_size, ack_queue_length, **kwargs):
    io_loop = ioloop.IOLoop.instance()

    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    state = State(
        event_log=event_log,
        io_loop=io_loop,
        chunk_size=chunk_size, ack_queue_length=ack_queue_length,
        service_id=service_id, source_id=source_id)
    state.start()
    io_loop.start()


def init(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.initialize()


def truncate(table_name, proxy_path, count, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.truncate(count)


def run():
    options.define("table_name",
        metavar="PATH",
        default=DEFAULT_TABLE_NAME,
        help="[yt] path to scheduler event log")
    options.define("proxy_path", metavar="URL", help="[yt] url to proxy")
    options.define("chunk_size", default=DEFAULT_CHUNK_SIZE, help="size of chunk in rows")
    options.define("ack_queue_length", default=DEFAULT_ACK_QUEUE_LENGTH, help="number of concurrent chunks to save")

    options.define("service_id", default=DEFAULT_SERVICE_ID, help="[logbroker] service id")
    options.define("source_id", default=DEFAULT_SOURCE_ID, help="[logbroker] source id")

    options.define("count", default=10**6, help="number of lines to truncate")

    options.define("init", default=False, help="init and exit")
    options.define("truncate", default=False, help="truncate and exit")

    options.define("log_dir", metavar="PATH", default="/var/log/fennel", help="log directory")
    options.define("verbose", default=False, help="vervose mode")

    options.parse_command_line()

    logging.debug("Started")

    @atexit.register
    def log_exit():
        logging.debug("Exited")

    if options.options.truncate:
        func = truncate
    elif options.options.init:
        func = init
    else:
        func = main

    func(**options.options.as_dict())


if __name__ == "__main__":
    run()
