import os
import pytest
import exts.path2
import exts.tmp
import library.python.windows


class TestPathPrefixes(object):
    @staticmethod
    def check(path, *args):
        assert tuple(exts.path2.path_prefixes(path)) == args

    def test_empty(self):
        self.check('')

    def test_simple(self):
        self.check('path', 'path')

    def test_root(self):
        self.check('/', '/')

    def test_relative(self):
        self.check('some/usual/path', 'some', os.path.join('some', 'usual'), os.path.join('some', 'usual', 'path'))

    def test_absolute(self):
        self.check(
            '/some/usual/path', '/', '/some', os.path.join('/some', 'usual'), os.path.join('/some', 'usual', 'path')
        )


@pytest.mark.parametrize(
    "given, expected",
    library.python.windows.on_win()
    and [
        ('\\', ['\\']),
        ('\\..', ['\\']),
        ('/path', ['\\', 'path']),
        ('/path/', ['\\', 'path']),
        ('/path/to', ['\\', 'path', 'to']),
        ('/path/to/some/directory', ['\\', 'path', 'to', 'some', 'directory']),
        ('/path/to/some/directory/../', ['\\', 'path', 'to', 'some']),
    ]
    or [
        ('/', ['/']),
        ('/..', ['/']),
        ('/path', ['/', 'path']),
        ('/path/', ['/', 'path']),
        ('/path/to', ['/', 'path', 'to']),
        ('/path/to/some/directory', ['/', 'path', 'to', 'some', 'directory']),
        ('/path/to/some/directory/../', ['/', 'path', 'to', 'some']),
    ],
)
def test_path_explode_absolute(given, expected):
    assert expected == exts.path2.path_explode(given)


@pytest.mark.parametrize(
    "given, expected",
    library.python.windows.on_win()
    and [
        ('', []),
        ('a/..', ['.']),
        ('path', ['path']),
        ('path/', ['path']),
        ('path/to', ['path', 'to']),
        ('path/to/some/directory', ['path', 'to', 'some', 'directory']),
        ('path/to/some/directory/../', ['path', 'to', 'some']),
    ]
    or [
        ('', []),
        ('a/..', ['.']),
        ('path', ['path']),
        ('path/', ['path']),
        ('path/to', ['path', 'to']),
        ('path/to/some/directory', ['path', 'to', 'some', 'directory']),
        ('path/to/some/directory/../', ['path', 'to', 'some']),
    ],
)
def test_path_explode_relative(given, expected):
    assert expected == exts.path2.path_explode(given)


class TestAbspath(object):
    def check(self, path, model):
        assert exts.path2.abspath(path) == model

    def check_based(self, path, base, model):
        assert exts.path2.abspath(path, base) == model

    def check_based_error(self, path, base):
        with pytest.raises(AssertionError):
            exts.path2.abspath(path, base)

    def test_simple(self):
        self.check('some/path', os.path.join(os.getcwd(), 'some/path'))
        self.check('.', os.getcwd())

    def test_abs(self):
        self.check('/some/path', '/some/path')
        self.check('/', '/')

    def test_based(self):
        self.check_based('some/path', '/base', '/base/some/path')
        self.check_based('some/path', '/', '/some/path')
        self.check_based('/some/path', '/base', '/some/path')

    def test_norm(self):
        self.check_based('some/../path', '/base', '/base/path')
        self.check_based('.', '/base', '/base')
        self.check_based('..', '/base', '/')

    @pytest.mark.skipif(library.python.windows.on_win(), reason='Symlinks disabled on Windows')
    def test_no_resolve(self):
        with exts.tmp.temp_dir() as tmp_path:
            dir_path = os.path.join(tmp_path, 'dir')
            link_path = os.path.join(tmp_path, 'link')
            exts.fs.create_dirs(dir_path)
            exts.fs.symlink(dir_path, link_path)
            self.check_based('dir', tmp_path, dir_path)
            self.check_based('link', tmp_path, link_path)

    def test_error(self):
        self.check_based_error('some/path', 'another/path')


class TestLocalpath(object):
    def check(self, path, base, model):
        assert exts.path2.localpath(path, base) == model

    def check_error(self, path, base):
        with pytest.raises(AssertionError):
            exts.path2.localpath(path, base)

    def test_simple(self):
        self.check('/base/some/path', '/base', 'some/path')
        self.check('/some/path', '/', 'some/path')
        self.check('/some/path', '/some/path', '.')

    def test_not_base(self):
        self.check('/base/some/path', '/base/other/path', None)

    def test_norm(self):
        self.check('/base/some/../path', '/base/other/../path', '.')

    @pytest.mark.skipif(library.python.windows.on_win(), reason='Symlinks disabled on Windows')
    def test_resolve(self):
        with exts.tmp.temp_dir() as tmp_path:
            dir_path = os.path.join(tmp_path, 'dir')
            link_path = os.path.join(tmp_path, 'link')
            exts.fs.create_dirs(dir_path)
            exts.fs.symlink(dir_path, link_path)
            self.check(dir_path, tmp_path, 'dir')
            self.check(link_path, tmp_path, 'dir')
            self.check(os.path.join(link_path, 'file'), dir_path, 'file')

    def test_error(self):
        self.check_error('base/some/path', '/base')
        self.check_error('/base/some/path', 'base')
        self.check_error('base/some/path', 'base')
