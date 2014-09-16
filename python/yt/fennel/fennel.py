#!/usr/bin/python

import yt.wrapper as yt

from tornado import ioloop
from tornado import iostream
from tornado import gen
from tornado import options

import requests

import atexit
import socket
import struct
import logging
import json
import datetime
import zlib
import sys


DEFAULT_TABLE_NAME = "//sys/scheduler/event_log"
DEFAULT_ADVICER_URL = "http://cellar-t.stat.yandex.net/advise"
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
                 **log_broker_options):
        self._chunk_size = chunk_size
        self._ack_queue_length = ack_queue_length
        self._last_saved_seqno = 0
        self._last_seqno = 0

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._event_log = event_log
        self._log_broker = LogBroker(self, self._io_loop, IOStreamClass, **log_broker_options)

        self._save_chunk_handle = None
        self._update_state_handle = None

    def start(self):
        self._initialize()
        self._log_broker.start()

    def abort(self):
        self._log_broker.abort()

    def _initialize(self):
        self._last_saved_seqno = self._from_row_number(self._event_log.get_next_row_to_save()) - 1
        self._last_seqno = self._last_saved_seqno
        self.log.info("Last acked seqno is %d", self._last_seqno)

    def maybe_save_another_chunk(self):
        if self._save_chunk_handle is not None:
            # wait for callback
            return
        if self._last_saved_seqno - self._last_seqno < self._ack_queue_length:
            self._save_chunk()

    def _save_chunk(self):
        self.log.debug("Schedule chunk save")
        self._save_chunk_handle = None
        try:
            seqno = self._last_saved_seqno + 1
            data = self._event_log.get_data(self._to_row_number(seqno), self._chunk_size)
            self._log_broker.save_chunk(seqno, data)
            self._last_saved_seqno = seqno
        except yt.YtError:
            self.log.error("Unable to schedule chunk save", exc_info=True)
            self._save_chunk_handle = self._io_loop.add_timeout(datetime.timedelta(seconds=1), self._save_chunk)
        except EventLog.NotEnoughDataError:
            self.log.warning("Unable to get {0} rows from event log".format(self._chunk_size), exc_info=True)
            self._io_loop.add_timeout(datetime.timedelta(seconds=120), self.maybe_save_another_chunk)
        else:
            self._io_loop.add_callback(self.maybe_save_another_chunk)

    def on_session_changed(self):
        self._last_saved_seqno = self._last_seqno
        self.log.info("Last acked seqno is %d", self._last_seqno)
        self.maybe_save_another_chunk()

    def on_skip(self, seqno):
        self.log.debug("Skip seqno=%d", seqno)
        if seqno > self._last_seqno:
            self.update_last_seqno(seqno)

    def on_save_ack(self, seqno):
        self.log.debug("Ack seqno=%d", seqno)
        if seqno > self._last_seqno:
            self.update_last_seqno(seqno)

    def update_last_seqno(self, new_last_seqno):
        self.log.debug("Update last seqno: %d", new_last_seqno)

        self._last_seqno = new_last_seqno
        self.log.info("Last acked seqno is %d", self._last_seqno)

        if self._update_state_handle is None:
            self._update_state_handle = self._io_loop.add_timeout(datetime.timedelta(seconds=5), self._update_state)

        self.maybe_save_another_chunk()

    def _update_state(self):
        self.log.debug("Update state. Last acked seqno: %d", self._last_seqno)
        self._update_state_handle = None
        try:
            self._event_log.set_next_row_to_save(self._to_row_number(self._last_seqno))
        except yt.YtError:
            self.log.error("Unable to update next row to save", exc_info=True)
            self._update_state_handle = self._io_loop.add_timeout(datetime.timedelta(seconds=1), self._update_state)

    def _to_row_number(self, reqno):
        return reqno * self._chunk_size

    def _from_row_number(self, row_number):
        return row_number / self._chunk_size


class EventLog(object):
    log = logging.getLogger("EventLog")

    class NotEnoughDataError(RuntimeError):
        pass

    def __init__(self, yt, table_name=None):
        self.yt = yt
        self._table_name = table_name or "//tmp/event_log"
        self._archive_table_name = self._table_name + ".archive"
        self._number_of_first_row_attr = "{0}/@number_of_first_row".format(self._table_name)
        self._row_to_save_attr = "{0}/@row_to_save".format(self._table_name)
        self._row_count = "{0}/@row_count".format(self._table_name)

    def get_data(self, begin, count):
        with self.yt.Transaction():
            rows_removed = self.yt.get(self._number_of_first_row_attr)
            begin -= rows_removed
            assert begin >= 0
            self.log.debug("Reading %s event log. Begin: %d, count: %d",
                self._table_name,
                begin,
                count)
            result = [item for item in self.yt.read_table(yt.TablePath(
                self._table_name,
                start_index=begin,
                end_index=begin + count), format="json", raw=False)]
            self.log.debug("Reading is finished")
        if len(result) != count:
            raise EventLog.NotEnoughDataError("Not enough data. Got only {0} rows".format(len(result)))
        return result

    def truncate(self, count):
        with self.yt.Transaction():
            first_row = self.yt.get(self._number_of_first_row_attr)
            first_row += count
            self.yt.set(self._number_of_first_row_attr, first_row)
            self.yt.run_erase(yt.TablePath(
                self._table_name,
                start_index=0,
                end_index=count))

    def monitor(self, threshold):
        with self.yt.Transaction():
            first_row = self.yt.get(self._number_of_first_row_attr)
            row_to_save = int(self.get_next_row_to_save())
            real_row_to_save = row_to_save - first_row

            row_count = self.yt.get(self._row_count)

            lag = row_count - real_row_to_save
            if lag > threshold:
                sys.stdout.write("2;  Lag equals to: %d\n" % (lag,))
            else:
                sys.stdout.write("0; Lag equals to: %d\n" % (lag,))

    def archive(self, count = None):
        desired_chunk_size = 2 * 1024 ** 3
        ratio = 0.137
        data_size_per_job = max(1, int(desired_chunk_size / ratio))

        count = count or yt.get(self._table_name + "/@row_count")

        self.log.info("Archive %s rows from event log", count)

        partition = yt.TablePath(
            self._table_name,
            start_index=0,
            end_index=count)

        with self.yt.Transaction():
            self.yt.create_table(
                self._archive_table_name,
                        attributes={
                            "erasure_codec": "lrc_12_2_2",
                            "compression_codec": "gzip_best_compression"
                        },
                        ignore_existing=True)

            self.log.info("Run merge...")
            self.yt.run_merge(
                source_table=partition,
                destination_table=yt.TablePath(self._archive_table_name, append=True),
                compression_codec="gzip_best_compression",
                spec={
                    "combine_chunks": "true",
                    "force_transform": "true",
                    "data_size_per_job": data_size_per_job,
                    "job_io": {
                        "table_writer": {
                            "desired_chunk_size": desired_chunk_size
                        }
                    }
                }
            )

        self.log.info("Truncate event log...")

        tries = 0
        finished = False
        backoff_time = 5
        while not finished:
            try:
                with self.yt.Transaction():
                    first_row = self.yt.get(self._number_of_first_row_attr)
                    first_row += count
                    self.yt.run_erase(partition)
                    self.yt.set(self._number_of_first_row_attr, first_row)
                finished = True
            except yt.common.YtError as e:
                self.log.error("Unhandled exception", exc_info=True)

                self.log.info("Retry again in %d seconds...", backoff_time)
                time.sleep(backoff_time)
                tries += 1
                backoff_time = min(backoff_time * 2, 180)

    def set_next_row_to_save(self, row_number):
        self.yt.set(self._row_to_save_attr, row_number)

    def get_next_row_to_save(self):
        return self.yt.get(self._row_to_save_attr)

    def initialize(self):
        with self.yt.Transaction():
            if not self.yt.exists(self._row_to_save_attr):
                self.yt.set(self._row_to_save_attr, 0)
            if not self.yt.exists(self._number_of_first_row_attr):
                self.yt.set(self._number_of_first_row_attr, 0)


def serialize_chunk(chunk_id, seqno, lines, data):
    serialized_data = struct.pack(CHUNK_HEADER_FORMAT, chunk_id, seqno, lines)
    serialized_data += zlib.compress("\n".join(["json-" + json.dumps(row).encode("string_escape") for row in data]))
    return serialized_data


def parse_chunk(serialized_data):
    serialized_data = serialized_data.strip()

    index = serialized_data.find("\r\n")
    assert index != -1
    index += len("\r\n")

    chunk_id, seqno, lines = struct.unpack(CHUNK_HEADER_FORMAT, serialized_data[index:index + CHUNK_HEADER_SIZE])
    index += CHUNK_HEADER_SIZE

    decompressed_data = zlib.decompress(serialized_data[index:])

    data = []
    for line in decompressed_data.split("\n"):
        data.append(json.loads(line[len("json-"):].decode("string_escape")))

    return data


class LogBroker(object):
    log = logging.getLogger("log_broker")

    def __init__(self, state, io_loop=None, IOStreamClass=None, advicer_url=None, **session_options):
        self._advicer_url = advicer_url
        self._state = state
        self._starting = False
        self._chunk_id = 0
        self._lines = 0
        self._push_channel = None
        self._session = None
        self._session_options = session_options
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def start(self):
        if not self._starting:
            self._starting = True
            self.log.info("Start a log broker")
            self._session = Session(self._state, self, self._io_loop, self.IOStreamClass, **self._session_options)
            self._session.connect()

    def abort(self):
        self._push_channel.abort()
        self._session.abort()

    def save_chunk(self, seqno, data):
        if self._push_channel is not None:
            serialized_data = serialize_chunk(self._chunk_id, seqno, self._lines, data)
            self._chunk_id += 1
            self._lines += 1

            self.log.debug("Save chunk [%d]", seqno)
            data_to_write = "{size:X}\r\n{data}\r\n".format(size=len(serialized_data), data=serialized_data)
            self._push_channel.write(data_to_write)
        else:
            assert False

    def on_session_changed(self, id_, host):
        self._starting = False
        if self._push_channel is not None:
            self._push_channel.abort()

        self._chunk_id = 0
        self._lines = 0

        self._push_channel = PushChannel(self._state, id_,
            io_loop=self._io_loop,
            IOStreamClass=self.IOStreamClass,
            endpoint=(host, 9000))
        self._push_channel.connect()

    def get_endpoint(self):
        self.log.info("Getting adviced logbroker endpoint...")
        response = requests.get(self._advicer_url, headers={"ClientHost": socket.getfqdn()})
        if not response.ok:
            self.log.error("Unable to get adviced logbroker endpoint")
            return None
        host = response.text.strip()

        self.log.info("Adviced endpoint: %s", host)
        return (host, 80)


class PushChannel(object):
    log = logging.getLogger("push_channel")

    def __init__(self, state, session_id, io_loop=None, IOStreamClass=None, endpoint=None):
        self._state = state
        self._session_id = session_id
        self._aborted = False

        self._endpoint = endpoint
        self._host = self._endpoint[0]

        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._iostream = None
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def connect(self):
        self.log.info("Create a push channel. Endpoint: %s. Session: %s", self._endpoint, self._session_id)
        if self._aborted:
            self.log.error("Unable to connect: channel is aborted")
            return
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
        self._iostream = self.IOStreamClass(s, io_loop=self._io_loop)
        self._iostream.set_close_callback(self.on_close)
        self._iostream.connect(self._endpoint, callback=self.on_connect)
        self.log.info("Send request")
        self._iostream.write(
            "PUT /rt/store HTTP/1.1\r\n"
            "Host: {host}\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Encoding: gzip\r\n"
            "Transfer-Encoding: chunked\r\n"
            "RTSTreamFormat: v2le\r\n"
            "Session: {session_id}\r\n"
            "\r\n".format(
                host=self._host,
                session_id=self._session_id)
        )

    def write(self, data):
        if self._aborted:
            self.log.error("Unable to write: channel is aborted")
            return
        self._iostream.write(data)

    def abort(self):
        self.log.info("Abort the push channel")
        self._aborted = True
        if self._iostream is not None:
            self._iostream.close()

    def on_connect(self):
        self.log.info("The push channel has been created")
        self._state.on_session_changed()
        self._iostream.read_until_close(self.on_response_end, self.on_response)

    def on_response(self, data):
        self.log.debug(data)

    def on_response_end(self, data):
        self.log.debug(data)

    def on_close(self):
        self.log.info("The push channel has been closed")
        self._iostream = None
        if not self._aborted:
            self._io_loop.add_timeout(datetime.timedelta(seconds=1), self.connect)


class Session(object):
    log = logging.getLogger("session")

    def __init__(
            self,
            state,
            log_broker,
            io_loop=None,
            IOStreamClass=None,
            service_id=None,
            source_id=None):
        self._endpoint = None
        self._host = None
        self._service_id = service_id or DEFAULT_SERVICE_ID
        self._source_id = source_id or DEFAULT_SOURCE_ID
        self._id = None
        self._aborted = False
        self._state = state
        self._log_broker = log_broker
        self._io_loop = io_loop or ioloop.IOLoop.instance()
        self._iostream = None
        self.IOStreamClass = IOStreamClass or iostream.IOStream

    def connect(self):
        assert self._iostream is None
        if self._aborted:
            return
        self.log.info("Connect to kafka")

        # check endpoint
        self._endpoint = self._log_broker.get_endpoint()
        if self._endpoint is None:
            self.log.info("Unable to get logbroker endpoint. Retry later.")
            self._io_loop.add_timeout(datetime.timedelta(seconds=1), self.connect)

        self._host = self._endpoint[0]

        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            self._iostream = self.IOStreamClass(s, io_loop=self._io_loop)
            self._iostream.set_close_callback(self.on_close)
            self._iostream.connect(self._endpoint, callback=self.on_connect)
        except Exception:
            self._iostream = None
            self.log.error("Unable to start connecting. Retry later.", exc_info=True)
            self._io_loop.add_timeout(datetime.timedelta(seconds=1), self.connect)

        self.log.info("Send request. Ident: %s. SourceId: %s. Endpoint: %s",
            self._service_id,
            self._source_id,
            self._endpoint)
        try:
            self._iostream.write(
                "GET /rt/session?"
                "ident={ident}&"
                "sourceid={source_id}&"
                "logtype=json "
                "HTTP/1.1\r\n"
                "Host: {host}\r\n"
                "Accept: */*\r\n\r\n".format(
                    ident=self._service_id,
                    source_id=self._source_id,
                    host=self._host)
                )
        except iostream.StreamClosedError:
            self.log.error("Session is closed before created. Reconnect", exc_info=True)
            raise

    @gen.coroutine
    def on_connect(self):
        self.log.info("The session channel has been created")
        metadata_raw = yield gen.Task(self._iostream.read_until, "\r\n\r\n")

        self.log.debug("Parse response %s", metadata_raw)
        result = self.read_metadata(metadata_raw[:-4])
        if not result:
            self.log.error("Unable to find Session header in the response")
            self._iostream.close()
            return

        try:
            while True:
                headers_raw = yield gen.Task(self._iostream.read_until, "\r\n")
                try:
                    body_size = int(headers_raw, 16)
                except ValueError:
                    self.log.error("Bad HTTP chunk header format")
                    self._iostream.close()
                    return
                if body_size == 0:
                    self.log.error("HTTP response is finished")
                    self._iostream.close()
                    return
                data = yield gen.Task(self._iostream.read_bytes, body_size + 2)

                self.log.debug("Process status: %s", data.strip())
                self.process_data(data.strip())
        except Exception:
            self.log.error("Unhandled exception. Close the push channel", exc_info=True)
            self._iostream.close()
            raise

    def read_metadata(self, data):
        for index, line in enumerate(data.split("\n")):
            if index > 0:
                key, value = line.split(":", 1)
                if key.strip() == "Session":
                    self._id = value.strip()
                    self.log.info("Session id: %s", self._id)
                    self._log_broker.on_session_changed(self._id, self._host)
                    return True
        return False

    def abort(self):
        self.log.info("Abort the session channel")
        self._aborted = True
        if self.iostream is not None:
            self._iostream.close()

    def on_close(self):
        self.log.error("The session channel has been closed")
        self._iostream = None
        if not self._aborted:
            self._io_loop.add_timeout(datetime.timedelta(seconds=1), self.connect)

    def process_data(self, data):
        if data.startswith("skip"):
            line = data[len("skip") + 1:]
            handler = self._state.on_skip
        else:
            line = data
            handler = self._state.on_save_ack

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


def main(table_name, proxy_path, service_id, source_id, chunk_size, ack_queue_length, advicer_url, **kwargs):
    io_loop = ioloop.IOLoop.instance()

    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    state = State(
        event_log=event_log,
        io_loop=io_loop,
        chunk_size=chunk_size, ack_queue_length=ack_queue_length,
        service_id=service_id, source_id=source_id, advicer_url = advicer_url)
    state.start()
    io_loop.start()


def init(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.initialize()


def truncate(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    count = int(kwargs.get("count", 10**6))
    event_log.truncate(count)


def monitor(table_name, proxy_path, threshold, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    event_log.monitor(threshold)


def archive(table_name, proxy_path, **kwargs):
    yt.config.set_proxy(proxy_path)
    event_log = EventLog(yt, table_name=table_name)
    count = kwargs.get("count", None)
    if count is not None:
        count = int(count)
    event_log.archive(count)


def run():
    options.define("table_name",
        metavar="PATH",
        default=DEFAULT_TABLE_NAME,
        help="[yt] path to scheduler event log")
    options.define("proxy_path", metavar="URL", help="[yt] url to proxy")
    options.define("chunk_size", default=DEFAULT_CHUNK_SIZE, help="size of chunk in rows")
    options.define("ack_queue_length", default=DEFAULT_ACK_QUEUE_LENGTH, help="number of concurrent chunks to save")

    options.define("advicer_url", default=DEFAULT_ADVICER_URL, help="[logbroker] url to get adviced kafka endpoint")
    options.define("service_id", default=DEFAULT_SERVICE_ID, help="[logbroker] service id")
    options.define("source_id", default=DEFAULT_SOURCE_ID, help="[logbroker] source id")

    options.define("count", default=None, help="number of rows to truncate")

    options.define("threshold", default=10**6, help="threshold of lag size to generate error")

    options.define("init", default=False, help="init and exit")
    options.define("truncate", default=False, help="truncate and exit")
    options.define("monitor", default=False, help="output status and exit")
    options.define("archive", default=False, help="archive and exit")

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
    elif options.options.monitor:
        func = monitor
    elif options.options.archive:
        func = archive
    else:
        func = main

    try:
        func(**options.options.as_dict())
    except Exception:
        logging.error("Unhandled exception: ", exc_info=True)


if __name__ == "__main__":
    run()
