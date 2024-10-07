class Request(object):
    def __init__(
        self,
        command_name,
        parameters=None,
        input_stream=None,
        output_stream=None,
        user=None,
        id=None,
        token=None,
        trace_id=None,
        service_ticket=None,
        user_tag=None
    ):
        self.command_name = command_name
        self.parameters = parameters
        self.input_stream = input_stream
        self.output_stream = output_stream
        self.user = user
        self.user_tag = user_tag
        self.id = id
        self.token = token
        self.trace_id = trace_id
        self.service_ticket = service_ticket
