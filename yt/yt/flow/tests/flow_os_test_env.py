"""Pytest plugin that lets Flow integration tests run under a plain opensource
pytest invocation, outside the ya-make test runtime they were written for.

The tests assume two ya-make facilities that plain pytest does not provide; this
plugin recreates both:

* ``yatest.common`` (``binary_path`` / ``source_path`` / ``output_path`` /
  ``context``) delegates to a "ya" plugin instance that ya-make installs on the
  pytest config. We build one (``yatest_lib.ya.Ya``) from the roots exported by
  ``run_tests.sh`` and register it via ``yatest.common.runtime._set_ya_config``.
* ``context.project_path`` is set by ya-make to the test module's
  arcadia-relative directory. Tests read it at import time (e.g.
  ``binary_path(f"{project_path}/../shuffle")``), so it is refreshed for every
  collected module.

The plugin also implements the opt-out blacklist: every collected test runs
unless its path matches ``_BLACKLIST``.
"""

import os

import pytest

from yatest_lib.ya import Ya
import yatest.common.runtime as runtime


def _require_env(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            "{} must be set by run_tests.sh before running the Flow opensource tests.".format(name)
        )
    return value


_SOURCE_ROOT = os.path.realpath(_require_env("YT_FLOW_SOURCE_ROOT"))
_BUILD_ROOT = os.path.realpath(_require_env("YT_FLOW_BUILD_ROOT"))
_OUTPUT_DIR = _require_env("YT_FLOW_TEST_SANDBOX")

# Opt-out blacklist: substrings matched against each test's node id (which
# starts with the file path, so a plain path substring blacklists a whole file
# or directory, and a "::" fragment blacklists a single test). A test runs
# unless its node id contains one of these entries.
#
# The entries themselves live next to the CI path list in run_tests.sh
# (FLOW_TESTS_BLACKLIST), so the whole opensource scope is defined in one
# place; this plugin only applies them. FLOW_TESTS_IGNORE_BLACKLIST=1 runs
# blacklisted tests anyway.
_BLACKLIST = tuple(os.environ.get("FLOW_TESTS_BLACKLIST", "").split())

_IGNORE_BLACKLIST = os.environ.get("FLOW_TESTS_IGNORE_BLACKLIST") == "1"

_ya = None


def _project_path_for(path):
    return os.path.relpath(os.path.dirname(os.path.realpath(str(path))), _SOURCE_ROOT)


def _refresh_project_path(path):
    # Tests read context.project_path at import time, so keep it pointing at the
    # module currently being collected/executed.
    if _ya is not None and str(path).endswith(".py"):
        _ya._context["project_path"] = _project_path_for(path)


class _Config:
    """Mirrors the config object the ya-make runtime places on pytest.

    yatest.common.process reads these attributes during process teardown, so
    the bare ``_set_ya_config(ya=...)`` shortcut (which sets only ``ya``) is
    not enough.
    """

    def __init__(self, ya):
        self.ya = ya
        self.collect_cores = False
        self.sanitizer_extra_checks = False


def pytest_configure(config):
    global _ya
    # The recipe env file carries YT_PROXY* / YT_TOKEN /
    # YT_PROXY_URL_ALIASING_CONFIG written by the local YT recipe; Ya applies
    # it to os.environ the same way the ya-make runtime does.
    _ya = Ya(
        source_root=_SOURCE_ROOT,
        build_root=_BUILD_ROOT,
        output_dir=_OUTPUT_DIR,
        env_file=os.environ.get("YT_FLOW_RECIPE_ENV_FILE"),
    )
    runtime._set_ya_config(config=_Config(_ya))
    # Markers the ya-make runtime registers via yt.test_helpers plugins. The
    # plugins themselves are not loaded: authors enforces a mark-everything
    # policy that internal CI already owns, and the others are no-ops for the
    # Flow test classes.
    config.addinivalue_line("markers", "authors(*authors): mark explicating test authors (owners)")
    config.addinivalue_line("markers", "skip_if(condition)")


def pytest_ignore_collect(collection_path, config):
    if _IGNORE_BLACKLIST:
        return None
    normalized = str(collection_path).replace(os.sep, "/")
    # Only file/directory-level entries (no "::") can skip a path during
    # collection; per-test entries are handled in pytest_collection_modifyitems.
    if any("::" not in entry and entry in normalized for entry in _BLACKLIST):
        return True
    # Return None (not False) so other collection filters still apply.
    return None


def pytest_collection_modifyitems(config, items):
    # Deselect blacklisted tests by node id. This is the only place a per-test
    # ("::") entry can act, and it also catches path entries for files already
    # imported. Kept as deselection (not skip) so blacklisted tests do not show
    # up as noise in the report.
    if _IGNORE_BLACKLIST:
        return
    kept, deselected = [], []
    for item in items:
        node_id = item.nodeid.replace(os.sep, "/")
        if any(entry in node_id for entry in _BLACKLIST):
            deselected.append(item)
        else:
            kept.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = kept


def pytest_collectstart(collector):
    path = getattr(collector, "path", None) or getattr(collector, "fspath", None)
    if path is not None:
        _refresh_project_path(path)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    _refresh_project_path(getattr(item, "path", None) or getattr(item, "fspath", None))
