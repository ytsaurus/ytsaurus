from ytlib_python import Driver, BufferedStream

import cStringIO
from cStringIO import StringIO

class Request(object):
    def __init__ (self, command_name, arguments=None, input_stream=None, output_stream=None):
        self.command_name = command_name
        self.arguments = arguments
        self.input_stream = input_stream
        self.output_stream = output_stream

def chunk_iter(stream, response, size):
    while True:
        if stream.empty() and response.is_set():
            break
        yield stream.read(size)


def make_request(driver, request):
    description = driver.get_description(request.command_name)

    if description.output_type() != "null" and request.output_stream is None:
        if request.command_name in ["read", "download"]:
            request.output_stream = BufferedStream(size=64 * 1024 * 1024)
            response = driver.execute(request)
            return chunk_iter(request.output_stream, response, 1024 * 1024)
        else:
            request.output_stream = StringIO()

    driver.execute(request).wait()
    if isinstance(request.output_stream, cStringIO.OutputType):
        return request.output_stream.getvalue()
