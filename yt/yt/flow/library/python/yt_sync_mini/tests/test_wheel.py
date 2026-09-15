"""Integrity test for the ``ytsaurus-flow-yt-sync-mini`` pip package.

The wheel is built by plain setuptools outside ya. This test runs the real
``build_py`` of ``setup.py``, imports the result without ``library.*`` modules
and checks it sees the same data as the ya build.
"""

import ast
import contextlib
import importlib.machinery
import os
import sys
from unittest import mock

# Setuptools must be imported first: it provides ``distutils`` on Python 3.12+.
import setuptools  # noqa: F401
from distutils.core import run_setup

import yatest.common

import yt.yson as yson

import yt.yt.flow.library.python.pipeline_tables as pipeline_tables
import yt.yt.flow.library.python.yt_sync_mini as yt_sync_mini

SETUP_DIR = "yt/python/packages/ytsaurus-flow-yt-sync-mini"
PACKAGES = {
    "yt.yt.flow.library.python.pipeline_tables": "yt/yt/flow/library/python/pipeline_tables",
    "yt.yt.flow.library.python.yt_sync_mini": "yt/yt/flow/library/python/yt_sync_mini",
}
# Non-Python files the wheel ships, relative to the build root.
EXTRA_FILES = ()
# Modules available next to the wheel: ytsaurus-client and the wheel itself.
ALLOWED_IMPORTS = ("yt.yson", "yt.wrapper", *PACKAGES)


def _build_lib(tmp_path):
    build_lib = str(tmp_path / "build_lib")
    cwd = os.getcwd()
    # ``package_dir`` in setup.py is relative to its directory.
    os.chdir(yatest.common.source_path(SETUP_DIR))
    try:
        run_setup("setup.py", script_args=["build_py", "--build-lib", build_lib], stop_after="run")
    finally:
        os.chdir(cwd)
    return build_lib


def _is_within(name, prefixes):
    return any(name == prefix or name.startswith(prefix + ".") for prefix in prefixes)


def _unguarded_imports(tree):
    """Yields absolute imports outside ``try`` bodies that handle an import error."""
    guarded = {
        id(node)
        for try_node in ast.walk(tree)
        if isinstance(try_node, ast.Try)
        and any(
            isinstance(handler.type, ast.Name) and handler.type.id in ("ImportError", "ModuleNotFoundError")
            for handler in try_node.handlers
        )
        for statement in try_node.body
        for node in ast.walk(statement)
    }
    for node in ast.walk(tree):
        if id(node) in guarded:
            continue
        if isinstance(node, ast.Import):
            yield from (alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.level == 0:
            yield node.module


class _BuildLibFinder:
    def __init__(self, build_lib):
        self._build_lib = build_lib

    def find_spec(self, name, path=None, target=None):
        if not _is_within(name, PACKAGES):
            return None
        parent_dir = os.path.join(self._build_lib, *name.split(".")[:-1])
        return importlib.machinery.PathFinder.find_spec(name, [parent_dir])


@contextlib.contextmanager
def _wheel_modules(build_lib):
    """Imports the wheel packages from ``build_lib`` while ``library.*`` is unavailable."""
    swapped = ("library", *PACKAGES)
    parent = sys.modules["yt.yt.flow.library.python"]
    saved_attributes = {name: getattr(parent, name) for name in ("pipeline_tables", "yt_sync_mini")}
    saved_modules = {name: sys.modules.pop(name) for name in list(sys.modules) if _is_within(name, swapped)}
    finder = _BuildLibFinder(build_lib)
    try:
        sys.modules["library"] = None
        sys.meta_path.insert(0, finder)
        yield tuple(importlib.import_module(package) for package in PACKAGES)
    finally:
        if finder in sys.meta_path:
            sys.meta_path.remove(finder)
        for name in [name for name in sys.modules if _is_within(name, swapped)]:
            del sys.modules[name]
        sys.modules.update(saved_modules)
        for name, value in saved_attributes.items():
            setattr(parent, name, value)


def _dump_data(pipeline_tables_module, yt_sync_mini_module):
    client = mock.MagicMock()
    yt_sync_mini_module.create_pipeline(client, "//home/pipeline", tablet_cell_bundle="bundle")
    exported = {name: getattr(pipeline_tables_module, name) for name in pipeline_tables_module.__all__}
    calls = [[name, list(args), kwargs] for name, args, kwargs in client.mock_calls]
    # ``yson.dumps`` keeps the yson attributes that ``==`` ignores.
    return yson.dumps({"exported": exported, "calls": calls})


def test_wheel_integrity(tmp_path):
    build_lib = _build_lib(tmp_path)

    built_files = {
        os.path.relpath(os.path.join(root, file), build_lib) for root, _, files in os.walk(build_lib) for file in files
    }
    expected_files = set(EXTRA_FILES)
    for source_dir in PACKAGES.values():
        expected_files.update(
            os.path.join(source_dir, file)
            for file in os.listdir(yatest.common.source_path(source_dir))
            if file.endswith(".py")
        )
    assert built_files == expected_files

    for file in sorted(built_files):
        if file.endswith(".py"):
            with open(os.path.join(build_lib, file)) as source:
                for module in _unguarded_imports(ast.parse(source.read())):
                    allowed = module.split(".")[0] in sys.stdlib_module_names or _is_within(module, ALLOWED_IMPORTS)
                    assert allowed, f"{file} imports {module}, which the wheel does not provide"

    with _wheel_modules(build_lib) as (wheel_pipeline_tables, wheel_yt_sync_mini):
        for module in (wheel_pipeline_tables, wheel_yt_sync_mini):
            assert module.__file__.startswith(build_lib)
        wheel_data = _dump_data(wheel_pipeline_tables, wheel_yt_sync_mini)

    assert wheel_data == _dump_data(pipeline_tables, yt_sync_mini)
