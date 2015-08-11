from yt.common import YtError

class Request(object):
    def __init__ (self, command_name, parameters=None, input_stream=None, output_stream=None, user=None):
        self.command_name = command_name
        self.parameters = parameters
        self.input_stream = input_stream
        self.output_stream = output_stream
        self.user = user

def chunk_iter(stream, response, size):
    while True:
        if response.is_set():
            if not response.is_ok():
                raise YtError(response.error())
            else:
                break
        yield stream.read(size)

    while not stream.empty():
        yield stream.read(size)

