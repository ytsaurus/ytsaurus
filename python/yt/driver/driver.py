from ytlib_python import Driver

class Request(object):
    def __init__ (self, command_name, arguments=None,
                  input_stream=None, output_stream=None,
                  input_format=None, output_format=None):
        self.command_name = command_name
        self.arguments = arguments
        self.input_stream = input_stream
        self.output_stream = output_stream
        self.input_format = input_format
        self.output_format = output_format
