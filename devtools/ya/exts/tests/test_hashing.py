# -*- coding: utf-8 -*-

import os
import pytest
import six

from exts import hashing
from exts import tmp, fs

content = "test" * 2**20


def test_md5_path_is_generated_by_content():
    with tmp.temp_dir() as temp_dir:

        test_file = os.path.join(temp_dir, "test")
        with open(test_file, "w") as f:
            f.write(content)

        assert hashing.md5_path(test_file) == hashing.md5_value(content)


def test_md5_path_dir_one_file():
    with tmp.temp_dir() as temp_dir:
        test_dir = os.path.join(temp_dir, "test")
        fs.ensure_dir(test_dir)
        with open(os.path.join(test_dir, "test"), "w") as f:
            f.write(content)

        assert hashing.md5_path(test_dir) == hashing.md5_value(content)


def test_md5_path_dir():
    with tmp.temp_dir() as temp_dir:
        test_dir = os.path.join(temp_dir, "test")
        fs.ensure_dir(test_dir)
        deeper_dir = os.path.join(test_dir, "deeper")
        fs.ensure_dir(deeper_dir)

        with open(os.path.join(test_dir, "one.txt"), "w") as f:
            f.write(content)

        with open(os.path.join(deeper_dir, "two.txt"), "w") as f:
            f.write(content)

        assert hashing.md5_path(test_dir) != hashing.md5_value(content)
        assert hashing.md5_path(test_dir) == hashing.md5_value(content + content)


@pytest.mark.parametrize('preproc', (six.ensure_str, six.ensure_binary, six.ensure_text, lambda x: x))
@pytest.mark.parametrize('s', ('2132', "1234" * 100, "test", u"hey hey unicode ✅ ❌ o my god"))
def test_py23_compatibility_md5(s, preproc):
    from hashlib import md5

    ext_lib_value = hashing.md5_value(preproc(s))
    if six.PY3:
        s = s.encode('utf-8')

    try:
        orig_value = md5(s).hexdigest()
    except UnicodeEncodeError:
        orig_value = md5(s.encode('utf-8')).hexdigest()

    assert ext_lib_value == orig_value

    return ext_lib_value
