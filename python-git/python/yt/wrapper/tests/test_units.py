from yt.wrapper.string_iter_io import StringIterIO

def test_string_list_io_read():
    strings = ["ab", "", "c", "de", ""]

    for add_eoln in [False, True]:
        io = StringIterIO(iter(strings), add_eoln)
        sep = "\n" if add_eoln else ""
        for c in sep.join(strings):
            assert c == io.read(1)

def test_string_list_io_readline():
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
