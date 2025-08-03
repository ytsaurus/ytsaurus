# Lexer Implementation

import re

from .utils import Str, classify, get_regexp_width, Py36, Serialize, suppress
from .exceptions import UnexpectedCharacters, LexError, UnexpectedToken

###{standalone
from copy import copy


class Pattern(Serialize):
    raw = None
    type = None

    def __init__(self, value, flags=(), raw=None):
        self.value = value
        self.flags = frozenset(flags)
        self.raw = raw

    def __repr__(self):
        return repr(self.to_regexp())

    # Pattern Hashing assumes all subclasses have a different priority!
    def __hash__(self):
        return hash((type(self), self.value, self.flags))

    def __eq__(self, other):
        return type(self) == type(other) and self.value == other.value and self.flags == other.flags

    def to_regexp(self):
        raise NotImplementedError()

    def min_width(self):
        raise NotImplementedError()

    def max_width(self):
        raise NotImplementedError()

    if Py36:
        # Python 3.6 changed syntax for flags in regular expression
        def _get_flags(self, value):
            for f in self.flags:
                value = ('(?%s:%s)' % (f, value))
            return value

    else:
        def _get_flags(self, value):
            for f in self.flags:
                value = ('(?%s)' % f) + value
            return value



class PatternStr(Pattern):
    __serialize_fields__ = 'value', 'flags'

    type = "str"

    def to_regexp(self):
        return self._get_flags(re.escape(self.value))

    @property
    def min_width(self):
        return len(self.value)
    max_width = min_width


class PatternRE(Pattern):
    __serialize_fields__ = 'value', 'flags', '_width'

    type = "re"

    def to_regexp(self):
        return self._get_flags(self.value)

    _width = None
    def _get_width(self):
        if self._width is None:
            self._width = get_regexp_width(self.to_regexp())
        return self._width

    @property
    def min_width(self):
        return self._get_width()[0]

    @property
    def max_width(self):
        return self._get_width()[1]


class TerminalDef(Serialize):
    __serialize_fields__ = 'name', 'pattern', 'priority'
    __serialize_namespace__ = PatternStr, PatternRE

    def __init__(self, name, pattern, priority=1):
        assert isinstance(pattern, Pattern), pattern
        self.name = name
        self.pattern = pattern
        self.priority = priority

    def __repr__(self):
        return '%s(%r, %r)' % (type(self).__name__, self.name, self.pattern)

    def user_repr(self):
        if self.name.startswith('__'): # We represent a generated terminal
            return self.pattern.raw or self.name
        else:
            return self.name


class Token(Str):
    """A string with meta-information, that is produced by the lexer.

    When parsing text, the resulting chunks of the input that haven't been discarded,
    will end up in the tree as Token instances. The Token class inherits from Python's ``str``,
    so normal string comparisons and operations will work as expected.

    Attributes:
        type: Name of the token (as specified in grammar)
        value: Value of the token (redundant, as ``token.value == token`` will always be true)
        pos_in_stream: The index of the token in the text
        line: The line of the token in the text (starting with 1)
        column: The column of the token in the text (starting with 1)
        end_line: The line where the token ends
        end_column: The next column after the end of the token. For example,
            if the token is a single character with a column value of 4,
            end_column will be 5.
        end_pos: the index where the token ends (basically ``pos_in_stream + len(token)``)
    """
    __slots__ = ('type', 'pos_in_stream', 'value', 'line', 'column', 'end_line', 'end_column', 'end_pos')

    def __new__(cls, type_, value, pos_in_stream=None, line=None, column=None, end_line=None, end_column=None, end_pos=None):
        try:
            self = super(Token, cls).__new__(cls, value)
        except UnicodeDecodeError:
            value = value.decode('latin1')
            self = super(Token, cls).__new__(cls, value)

        self.type = type_
        self.pos_in_stream = pos_in_stream
        self.value = value
        self.line = line
        self.column = column
        self.end_line = end_line
        self.end_column = end_column
        self.end_pos = end_pos
        return self

    def update(self, type_=None, value=None):
        return Token.new_borrow_pos(
            type_ if type_ is not None else self.type,
            value if value is not None else self.value,
            self
        )

    @classmethod
    def new_borrow_pos(cls, type_, value, borrow_t):
        return cls(type_, value, borrow_t.pos_in_stream, borrow_t.line, borrow_t.column, borrow_t.end_line, borrow_t.end_column, borrow_t.end_pos)

    def __reduce__(self):
        return (self.__class__, (self.type, self.value, self.pos_in_stream, self.line, self.column))

    def __repr__(self):
        return 'Token(%r, %r)' % (self.type, self.value)

    def __deepcopy__(self, memo):
        return Token(self.type, self.value, self.pos_in_stream, self.line, self.column)

    def __eq__(self, other):
        if isinstance(other, Token) and self.type != other.type:
            return False

        return Str.__eq__(self, other)

    __hash__ = Str.__hash__


class LineCounter:
    __slots__ = 'char_pos', 'line', 'column', 'line_start_pos', 'newline_char'

    def __init__(self, newline_char):
        self.newline_char = newline_char
        self.char_pos = 0
        self.line = 1
        self.column = 1
        self.line_start_pos = 0

    def feed(self, token, test_newline=True):
        """Consume a token and calculate the new line & column.

        As an optional optimization, set test_newline=False if token doesn't contain a newline.
        """
        if test_newline:
            newlines = token.count(self.newline_char)
            if newlines:
                self.line += newlines
                self.line_start_pos = self.char_pos + token.rindex(self.newline_char) + 1

        self.char_pos += len(token)
        self.column = self.char_pos - self.line_start_pos + 1


class UnlessCallback:
    def __init__(self, mres):
        self.mres = mres

    def __call__(self, t):
        for mre, type_from_index in self.mres:
            m = mre.match(t.value)
            if m:
                t.type = type_from_index[m.lastindex]
                break
        return t


class CallChain:
    def __init__(self, callback1, callback2, cond):
        self.callback1 = callback1
        self.callback2 = callback2
        self.cond = cond

    def __call__(self, t):
        t2 = self.callback1(t)
        return self.callback2(t) if self.cond(t2) else t2


def _create_unless(terminals, g_regex_flags, re_, use_bytes):
    tokens_by_type = classify(terminals, lambda t: type(t.pattern))
    assert len(tokens_by_type) <= 2, tokens_by_type.keys()
    embedded_strs = set()
    callback = {}
    for retok in tokens_by_type.get(PatternRE, []):
        unless = []
        for strtok in tokens_by_type.get(PatternStr, []):
            if strtok.priority > retok.priority:
                continue
            s = strtok.pattern.value
            m = re_.match(retok.pattern.to_regexp(), s, g_regex_flags)
            if m and m.group(0) == s:
                unless.append(strtok)
                if strtok.pattern.flags <= retok.pattern.flags:
                    embedded_strs.add(strtok)
        if unless:
            callback[retok.name] = UnlessCallback(build_mres(unless, g_regex_flags, re_, match_whole=True, use_bytes=use_bytes))

    terminals = [t for t in terminals if t not in embedded_strs]
    return terminals, callback


def _build_mres(terminals, max_size, g_regex_flags, match_whole, re_, use_bytes):
    # Python sets an unreasonable group limit (currently 100) in its re module
    # Worse, the only way to know we reached it is by catching an AssertionError!
    # This function recursively tries less and less groups until it's successful.
    postfix = '$' if match_whole else ''
    mres = []
    while terminals:
        pattern = u'|'.join(u'(?P<%s>%s)' % (t.name, t.pattern.to_regexp() + postfix) for t in terminals[:max_size])
        if use_bytes:
            pattern = pattern.encode('latin-1')
        try:
            mre = re_.compile(pattern, g_regex_flags)
        except AssertionError:  # Yes, this is what Python provides us.. :/
            return _build_mres(terminals, max_size//2, g_regex_flags, match_whole, re_, use_bytes)

        mres.append((mre, {i: n for n, i in mre.groupindex.items()}))
        terminals = terminals[max_size:]
    return mres


def build_mres(terminals, g_regex_flags, re_, use_bytes, match_whole=False):
    return _build_mres(terminals, len(terminals), g_regex_flags, match_whole, re_, use_bytes)


def _regexp_has_newline(r):
    r"""Expressions that may indicate newlines in a regexp:
        - newlines (\n)
        - escaped newline (\\n)
        - anything but ([^...])
        - any-char (.) when the flag (?s) exists
        - spaces (\s)
    """
    return '\n' in r or '\\n' in r or '\\s' in r or '[^' in r or ('(?s' in r and '.' in r)


class Lexer(object):
    """Lexer interface

    Method Signatures:
        lex(self, text) -> Iterator[Token]
    """
    lex = NotImplemented

    def make_lexer_state(self, text):
        line_ctr = LineCounter(b'\n' if isinstance(text, bytes) else '\n')
        return LexerState(text, line_ctr)


class TraditionalLexer(Lexer):

    def __init__(self, conf):
        terminals = list(conf.terminals)
        assert all(isinstance(t, TerminalDef) for t in terminals), terminals

        self.re = conf.re_module

        if not conf.skip_validation:
            # Sanitization
            for t in terminals:
                try:
                    self.re.compile(t.pattern.to_regexp(), conf.g_regex_flags)
                except self.re.error:
                    raise LexError("Cannot compile token %s: %s" % (t.name, t.pattern))

                if t.pattern.min_width == 0:
                    raise LexError("Lexer does not allow zero-width terminals. (%s: %s)" % (t.name, t.pattern))

            if not (set(conf.ignore) <= {t.name for t in terminals}):
                raise LexError("Ignore terminals are not defined: %s" % (set(conf.ignore) - {t.name for t in terminals}))

        # Init
        self.newline_types = frozenset(t.name for t in terminals if _regexp_has_newline(t.pattern.to_regexp()))
        self.ignore_types = frozenset(conf.ignore)

        terminals.sort(key=lambda x: (-x.priority, -x.pattern.max_width, -len(x.pattern.value), x.name))
        self.terminals = terminals
        self.user_callbacks = conf.callbacks
        self.g_regex_flags = conf.g_regex_flags
        self.use_bytes = conf.use_bytes
        self.terminals_by_name = conf.terminals_by_name

        self._mres = None

    def _build(self):
        terminals, self.callback = _create_unless(self.terminals, self.g_regex_flags, self.re, self.use_bytes)
        assert all(self.callback.values())

        for type_, f in self.user_callbacks.items():
            if type_ in self.callback:
                # Already a callback there, probably UnlessCallback
                self.callback[type_] = CallChain(self.callback[type_], f, lambda t: t.type == type_)
            else:
                self.callback[type_] = f

        self._mres = build_mres(terminals, self.g_regex_flags, self.re, self.use_bytes)

    @property
    def mres(self):
        if self._mres is None:
            self._build()
        return self._mres

    def match(self, text, pos):
        for mre, type_from_index in self.mres:
            m = mre.match(text, pos)
            if m:
                return m.group(0), type_from_index[m.lastindex]

    def lex(self, state, parser_state):
        with suppress(EOFError):
            while True:
                yield self.next_token(state, parser_state)

    def next_token(self, lex_state, parser_state=None):
        line_ctr = lex_state.line_ctr
        while line_ctr.char_pos < len(lex_state.text):
            res = self.match(lex_state.text, line_ctr.char_pos)
            if not res:
                allowed = {v for m, tfi in self.mres for v in tfi.values()} - self.ignore_types
                if not allowed:
                    allowed = {"<END-OF-FILE>"}
                raise UnexpectedCharacters(lex_state.text, line_ctr.char_pos, line_ctr.line, line_ctr.column,
                                           allowed=allowed, token_history=lex_state.last_token and [lex_state.last_token],
                                           state=parser_state, terminals_by_name=self.terminals_by_name)

            value, type_ = res

            if type_ not in self.ignore_types:
                t = Token(type_, value, line_ctr.char_pos, line_ctr.line, line_ctr.column)
                line_ctr.feed(value, type_ in self.newline_types)
                t.end_line = line_ctr.line
                t.end_column = line_ctr.column
                t.end_pos = line_ctr.char_pos
                if t.type in self.callback:
                    t = self.callback[t.type](t)
                    if not isinstance(t, Token):
                        raise LexError("Callbacks must return a token (returned %r)" % t)
                lex_state.last_token = t
                return t
            else:
                if type_ in self.callback:
                    t2 = Token(type_, value, line_ctr.char_pos, line_ctr.line, line_ctr.column)
                    self.callback[type_](t2)
                line_ctr.feed(value, type_ in self.newline_types)

        # EOF
        raise EOFError(self)


class LexerState:
    __slots__ = 'text', 'line_ctr', 'last_token'

    def __init__(self, text, line_ctr, last_token=None):
        self.text = text
        self.line_ctr = line_ctr
        self.last_token = last_token

    def __copy__(self):
        return type(self)(self.text, copy(self.line_ctr), self.last_token)


class ContextualLexer(Lexer):

    def __init__(self, conf, states, always_accept=()):
        terminals = list(conf.terminals)
        terminals_by_name = conf.terminals_by_name

        trad_conf = copy(conf)
        trad_conf.terminals = terminals

        lexer_by_tokens = {}
        self.lexers = {}
        for state, accepts in states.items():
            key = frozenset(accepts)
            try:
                lexer = lexer_by_tokens[key]
            except KeyError:
                accepts = set(accepts) | set(conf.ignore) | set(always_accept)
                lexer_conf = copy(trad_conf)
                lexer_conf.terminals = [terminals_by_name[n] for n in accepts if n in terminals_by_name]
                lexer = TraditionalLexer(lexer_conf)
                lexer_by_tokens[key] = lexer

            self.lexers[state] = lexer

        assert trad_conf.terminals is terminals
        self.root_lexer = TraditionalLexer(trad_conf)

    def make_lexer_state(self, text):
        return self.root_lexer.make_lexer_state(text)

    def lex(self, lexer_state, parser_state):
        try:
            while True:
                lexer = self.lexers[parser_state.position]
                yield lexer.next_token(lexer_state, parser_state)
        except EOFError:
            pass
        except UnexpectedCharacters as e:
            # In the contextual lexer, UnexpectedCharacters can mean that the terminal is defined, but not in the current context.
            # This tests the input against the global context, to provide a nicer error.
            try:
                last_token = lexer_state.last_token  # Save last_token. Calling root_lexer.next_token will change this to the wrong token
                token = self.root_lexer.next_token(lexer_state, parser_state)
                raise UnexpectedToken(token, e.allowed, state=parser_state, token_history=[last_token], terminals_by_name=self.root_lexer.terminals_by_name)
            except UnexpectedCharacters:
                raise e  # Raise the original UnexpectedCharacters. The root lexer raises it with the wrong expected set.

class LexerThread:
    """A thread that ties a lexer instance and a lexer state, to be used by the parser"""

    def __init__(self, lexer, text):
        self.lexer = lexer
        self.state = lexer.make_lexer_state(text)

    def lex(self, parser_state):
        return self.lexer.lex(self.state, parser_state)
###}
