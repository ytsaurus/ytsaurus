from threading import Thread
from Queue import Queue

import struct
import cPickle as pickle

class ReadingThread(Thread):
    def __init__(self, pipe, queue):
        super(ReadingThread, self).__init__()
        self.pipe = pipe
        self.queue = queue

    def run(self):
        while True:
            length_str = self.pipe.read(4)
            if not length_str:
                return
            if len(length_str) != 4:
                raise RuntimeError("Incorrect message format: expected 4 bytes, but  for %d bytes found" % length_str)
            length = struct.unpack("i", length_str)[0]
            self.queue.put(pickle.loads(self.pipe.read(length)))

# TODO(ignat): limit queue only to call get(), get_nowait(), empty()
class MessageReader(Queue):
    def __init__(self, pipe):
        Queue.__init__(self)
        self.thread = ReadingThread(pipe, self)
        self.thread.start()

    def finished(self):
        return not self.thread.is_alive() and self.empty()

class MessageWriter(object):
    def __init__(self, stream):
        self.stream = stream

    def put(self, obj):
        message = pickle.dumps(obj)
        self.stream.write(struct.pack("i", len(message)))
        self.stream.write(message)
        if hasattr(self.stream, "flush"):
            self.stream.flush()
