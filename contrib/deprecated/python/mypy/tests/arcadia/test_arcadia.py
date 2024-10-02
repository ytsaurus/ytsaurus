import yatest.common as yac
import pytest
import os

FILES = [("""
def p() -> None:
    print('hello')

a = p()  # Error: "p" does not return a value
         """, b'error: "p" does not return a value', 1),
         ("""
from typing import List

def greet_all(names: List[str]) -> None:
    for name in names:
        print('Hello ' + name)

names = ["Alice", "Bob", "Charlie"]
ages = [10, 20, 30]

greet_all(names)   # Ok!
greet_all(ages)    # Error due to incompatible types
          """, b'error: Argument 1 to "greet_all" has incompatible type "List[int]"; expected "List[str]"', 2)]


@pytest.mark.parametrize('file', FILES)
def test_builtin(file):
    c, err, id = file
    mypy = yac.binary_path("contrib/deprecated/python/mypy/bin/mypy/mypy")
    builtin_file_name = 'builtin{}.py'.format(id)

    with open(builtin_file_name, 'w') as f:
        f.write(c)

    assert os.path.exists(mypy)
    assert os.path.exists(builtin_file_name)

    p = yac.execute([mypy, builtin_file_name, '-vvv'],
                    check_exit_code=False)
    assert p.exit_code == 1, p.std_out + p.std_err
    assert err in p.std_out + p.std_err
