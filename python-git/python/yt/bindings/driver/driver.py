from driver_lib import BufferedStream

from yt.common import YtError
from yt.yson.convert import to_yson_type

import cStringIO
from cStringIO import StringIO

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


def make_request(driver, request):
    description = driver.get_command_descriptor(request.command_name)

    yson_format = to_yson_type("yson", attributes={"format": "text"})

    if description.input_type() != "null" and request.parameters.get("input_format") is None:
        request.parameters["input_format"] = yson_format

    if description.output_type() != "null" and request.output_stream is None:
        if request.parameters.get("output_format") is None:
            request.parameters["output_format"] = yson_format
        if request.command_name in ["read", "download"]:
            request.output_stream = BufferedStream(size=64 * 1024 * 1024)
            response = driver.execute(request)
            return chunk_iter(request.output_stream, response, 1024 * 1024)
        else:
            request.output_stream = StringIO()

    response = driver.execute(request)
    response.wait()
    if not response.is_ok():
        raise YtError(response.error())
    if isinstance(request.output_stream, cStringIO.OutputType):
        return request.output_stream.getvalue()
