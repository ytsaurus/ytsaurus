# Look https://docs.python.org/2/howto/logging-cookbook.html#logging-to-a-single-file-from-multiple-processes for details

import pickle
import select
import logging
import logging.handlers
import SocketServer
import struct

class LogRecordStreamHandlerBase(SocketServer.StreamRequestHandler):
    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            chunk_len = struct.unpack('>L', chunk)[0]
            chunk = self.connection.recv(chunk_len)
            while len(chunk) < chunk_len:
                chunk = chunk + self.connection.recv(chunk_len - len(chunk))
            obj = pickle.loads(chunk)
            record = logging.makeLogRecord(obj)
            self.handle_log_record(record)

class LogRecordSocketReceiver(SocketServer.ThreadingTCPServer):
    allow_reuse_address = 1

    def __init__(self,
                 record_handler,
                 host='localhost',
                 port=logging.handlers.DEFAULT_TCP_LOGGING_PORT):

        def handle(record):
            record_handler(record)

        LogRecordStreamHandler = type(
                "LogRecordStreamHandler",
                (LogRecordStreamHandlerBase, object), 
                {"handle_log_record": lambda self, record: handle(record)})

        SocketServer.ThreadingTCPServer.__init__(self, (host, port), LogRecordStreamHandler)
        self.timeout = 1

    def serve_until_stopped(self):
        while True:
            rd, wr, ex = select.select([self.socket.fileno()],
                                       [], [],
                                       self.timeout)
            if rd:
                self.handle_request()

