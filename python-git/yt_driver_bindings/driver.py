
class Request(object):
    def __init__ (self, command_name, parameters=None, input_stream=None, output_stream=None, user=None, id=None):
        self.command_name = command_name
        self.parameters = parameters
        self.input_stream = input_stream
        self.output_stream = output_stream
        self.user = user
        self.id = id
