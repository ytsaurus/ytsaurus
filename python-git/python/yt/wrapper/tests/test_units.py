from yt.wrapper.string_iter_io import StringIterIO
import yt.wrapper as yt

from cStringIO import StringIO

def test_string_iter_io_read():
    strings = ["ab", "", "c", "de", ""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = "\n" if add_eoln else ""
        for c in sep.join(strings):
            assert c == io.read(1)

def test_string_iter_io_readline():
    strings = ["ab", "", "c\n", "d\ne", ""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = "\n" if add_eoln else ""
        lines = sep.join(strings).split("\n")
        for i in xrange(len(lines)):
            line = lines[i]
            if i + 1 != len(lines):
                line += "\n"
            assert line == io.readline()

class TestFormats(object):
    def _check(self, format, string, row):
        assert format.loads_row(string) == row
        assert format.load_row(StringIO(string)) == row
        assert list(format.load_rows(StringIO(string))) == [row]

        assert format.dumps_row(row) == string
        stream = StringIO()
        format.dump_row(row, stream)
        assert stream.getvalue().rstrip("\n") == string.rstrip("\n")

    def test_dsv_format(self):
        format = yt.DsvFormat()
        self._check(format, "a=1\tb=2\n", {"a": "1", "b": "2"})
        self._check(format, "a=1\\t\tb=2\n", {"a": "1\t", "b": "2"})

    def test_yamr_format(self):
        format = yt.YamrFormat(lenval=False, has_subkey=False)
        self._check(format, "a\tb\n", yt.Record("a", "b"))
        self._check(format, "a\tb\tc\n", yt.Record("a", "b\tc"))

        format = yt.YamrFormat(lenval=False, has_subkey=True)
        self._check(format, "a\tb\tc\n", yt.Record("a", "b", "c"))
        self._check(format, "a\t\tc\td\n", yt.Record("a", "", "c\td"))

        format = yt.YamrFormat(lenval=True, has_subkey=False)
        # TODO(ignat): why 1 bit is forward?
        self._check(format, "\x01\x00\x00\x00a\x01\x00\x00\x00b", yt.Record("a", "b"))

    def test_json_format(self):
        format = yt.JsonFormat()
        self._check(format, '{"a": 1}', {"a": 1})

        stream = StringIO('{"a": 1}\n{"b": 2}')
        assert list(format.load_rows(stream)) == [{"a": 1}, {"b": 2}]

    def test_schemaful_dsv_format(self):
        format = yt.SchemafulDsvFormat(columns=["a", "b"])
        self._check(format, "1\t2\n", {"a": "1", "b": "2"})
        self._check(format, "\\n\t2\n", {"a": "\n", "b": "2"})

    def test_yamred_dsv_format(self):
        format = yt.YamredDsvFormat(key_column_names=["a", "b"], subkey_column_names=["c"], has_subkey=False, lenval=False)
        self._check(format, "a\tb\n", yt.Record("a", "b"))
        self._check(format, "a\tb\tc\n", yt.Record("a", "b\tc"))


