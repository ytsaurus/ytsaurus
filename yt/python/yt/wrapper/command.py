"""YT command misc"""


class Command(object):
    """Properties of YT command."""
    def __init__(self, name, input_type, output_type, is_volatile, is_heavy):
        self.name = name
        self.input_type = input_type
        self.output_type = output_type
        self.is_volatile = is_volatile
        self.is_heavy = is_heavy

    def http_method(self):
        if self.input_type is not None:
            return "PUT"
        elif self.is_volatile:
            return "POST"
        else:
            return "GET"


def parse_commands(description):
    """Parse tree-like description from /api response into commands dictionary."""
    commands = {}
    for elem in description:
        name = elem["name"]
        del elem["name"]

        for key in elem:
            if elem[key] == "null":
                elem[key] = None

        commands[name] = Command(name, **elem)
    return commands
