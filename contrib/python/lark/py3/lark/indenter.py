"Provides Indentation services for languages with indentation similar to Python"

from .exceptions import LarkError
from .lexer import Token

###{standalone
class DedentError(LarkError):
    pass

class Indenter:
    def __init__(self):
        self.paren_level = None
        self.indent_level = None
        assert self.tab_len > 0

    def handle_NL(self, token):
        if self.paren_level > 0:
            return

        yield token

        indent_str = token.rsplit('\n', 1)[1] # Tabs and spaces
        indent = indent_str.count(' ') + indent_str.count('\t') * self.tab_len

        if indent > self.indent_level[-1]:
            self.indent_level.append(indent)
            yield Token.new_borrow_pos(self.INDENT_type, indent_str, token)
        else:
            while indent < self.indent_level[-1]:
                self.indent_level.pop()
                yield Token.new_borrow_pos(self.DEDENT_type, indent_str, token)

            if indent != self.indent_level[-1]:
                raise DedentError('Unexpected dedent to column %s. Expected dedent to %s' % (indent, self.indent_level[-1]))

    def _process(self, stream):
        for token in stream:
            if token.type == self.NL_type:
                for t in self.handle_NL(token):
                    yield t
            else:
                yield token

            if token.type in self.OPEN_PAREN_types:
                self.paren_level += 1
            elif token.type in self.CLOSE_PAREN_types:
                self.paren_level -= 1
                assert self.paren_level >= 0

        while len(self.indent_level) > 1:
            self.indent_level.pop()
            yield Token(self.DEDENT_type, '')

        assert self.indent_level == [0], self.indent_level

    def process(self, stream):
        self.paren_level = 0
        self.indent_level = [0]
        return self._process(stream)

    # XXX Hack for ContextualLexer. Maybe there's a more elegant solution?
    @property
    def always_accept(self):
        return (self.NL_type,)

###}
