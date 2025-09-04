import pytest
import six
import yaml

from devtools.ya.test.programs.test_tool.lib.migrations_config import MigrationsConfig


MIGRATIONS_CONFIG = """
    flake8:
        F401:
            ignore:
            - F401
            prefixes:
            - abc
        F402:
            ignore:
            - F402
            prefixes:
            - abc/def
        F403:
            ignore:
            - F403
            prefixes:
            - abd
        F404_and_F405:
            ignore:
            - F404
            - F405
            prefixes:
            - abe
            - abf
        no_lint:
            ignore:
            - '*'
            prefixes:
            - skip/me/please
"""


@pytest.fixture
def config():
    return yaml.safe_load(six.StringIO(MIGRATIONS_CONFIG))


@pytest.mark.parametrize(
    "path_prefix, expected",
    [
        ("ab", set()),
        ("abg", set()),
        ("abc", {"F401"}),
        ("abc/def", {"F401", "F402"}),
        ("abc/def/ghi", {"F401", "F402"}),
        ("abd", {"F403"}),
        ("abd/efg/hij", {"F403"}),
        ("abe", {"F404", "F405"}),
        ("abf", {"F404", "F405"}),
        ("abf/abe", {"F404", "F405"}),
    ],
)
def test_exceptions(config, path_prefix, expected):
    config = MigrationsConfig(config)
    got = config.get_exceptions(path_prefix)
    assert got == expected


@pytest.mark.parametrize(
    "path_prefix, expected",
    [
        ("abc", False),
        ("skip/me", False),
        ("skip/me/please", True),
        ("skip/me/please/please", True),
    ],
)
def test_skipped(config, path_prefix, expected):
    config = MigrationsConfig(config)
    got = config.is_skipped(path_prefix)
    assert got is expected
