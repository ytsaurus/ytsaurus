from threading import Thread
from Queue import Queue

import os
import struct
import fcntl
import cPickle as pickle
import time

class Stream(object):
    def __init__(self):
        self.blocks = []
        self.length = 0
        self.index = 0
        self.pos = 0

    def add(self, block):
        self.blocks.append(block)
        self.length += len(block)

    def available(self):
        return self.length

    def extract(self, size):
        assert size <= self.available()
        original_size = size

        result = []
        while size > 0:
            block = self.blocks[self.index]
            if size >= len(block) - self.pos:
                result.append(block[self.pos:])
                size -= len(block) - self.pos
                self.pos = 0
                self.index += 1
            else:
                result.append(block[self.pos:self.pos + size])
                self.pos += size
                size = 0

        self.length -= original_size

        return "".join(result)


class ReadingThread(Thread):
    def __init__(self, pipe, queue, sleep_timeout):
        super(ReadingThread, self).__init__()
        self.pipe = pipe
        self.queue = queue
        self.sleep_timeout = sleep_timeout

        self.finished = False
        self.stream = Stream()
        self.length = None

        self._set_non_blocking()

    def _set_non_blocking(self):
        fd = self.pipe.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)

    def run(self):
        not_enough_bytes = False
        while not self.finished:
            if not_enough_bytes:
                time.sleep(self.sleep_timeout)

            try:
                self.stream.add(self.pipe.read())
            except IOError:
                pass

            not_enough_bytes = False
            available = self.stream.available()
            if self.length is None:
                if available < 4:
                    not_enough_bytes = True
                    continue
                length_str = self.stream.extract(4)
                self.length = struct.unpack("i", length_str)[0]
            else:
                if available < self.length:
                    not_enough_bytes = True
                    continue
                self.queue.put(pickle.loads(self.stream.extract(self.length)))
                self.length = None

    def finish(self):
        self.finished = True

# TODO(ignat): limit queue only to call get(), get_nowait(), empty()
class MessageReader(Queue):
    def __init__(self, pipe, sleep_timeout):
        Queue.__init__(self)
        self.thread = ReadingThread(pipe, self, sleep_timeout)
        self.thread.start()

    def finished(self):
        return not self.thread.is_alive() and self.empty()

    def delete(self):
        self.thread.finish()
        del self.__dict__["thread"]

class MessageWriter(object):
    def __init__(self, stream):
        self.stream = stream

    def put(self, obj):
        message = pickle.dumps(obj)
        self.stream.write(struct.pack("i", len(message)))
        self.stream.write(message)
        if hasattr(self.stream, "flush"):
            self.stream.flush()
