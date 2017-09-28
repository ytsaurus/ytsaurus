from __future__ import print_function

from yt.packages.six import iteritems, integer_types, text_type, binary_type, b
from yt.packages.six.moves import map as imap, xrange

import yt.yson as yson
import yt.subprocess_wrapper as subprocess

import yt.wrapper as yt

import pytest

import collections
import glob
import os
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager

TEST_DIR = "//home/wrapper_tests"

TESTS_LOCATION = os.path.dirname(os.path.abspath(__file__))
PYTHONPATH = os.path.abspath(os.path.join(TESTS_LOCATION, "../../../"))
TESTS_SANDBOX = os.environ.get("TESTS_SANDBOX", TESTS_LOCATION + ".sandbox")
ENABLE_JOB_CONTROL = bool(int(os.environ.get("TESTS_JOB_CONTROL", False)))

def get_test_dir_path():
    return os.path.join(TESTS_LOCATION, "files")

def get_test_file_path(name):
    return os.path.join(get_test_dir_path(), name)

@contextmanager
def set_config_option(name, value, final_action=None):
    old_value = yt.config._get(name)
    try:
        yt.config._set(name, value)
        yield
    finally:
        if final_action is not None:
            final_action()
        yt.config._set(name, old_value)

@contextmanager
def set_config_options(options_dict):
    old_values = {}
    for key in options_dict:
        old_values[key] = yt.config._get(key)
    try:
        for key, value in iteritems(options_dict):
            yt.config._set(key, value)
        yield
    finally:
        for key, value in iteritems(old_values):
            yt.config._set(key, value)

# Check equality of records
def check(rowsA, rowsB, ordered=True):
    def prepare(rows):
        def fix_unicode(obj):
            if isinstance(obj, text_type):
                return str(obj)
            return obj
        def process_row(row):
            if isinstance(row, dict):
                return dict([(fix_unicode(key), fix_unicode(value)) for key, value in iteritems(row)])
            return row

        rows = list(imap(process_row, rows))
        if not ordered:
            rows = tuple(sorted(imap(lambda obj: tuple(sorted(iteritems(obj))), rows)))

        return rows

    lhs, rhs = prepare(rowsA), prepare(rowsB)
    assert lhs == rhs

def _filter_simple_types(obj):
    if isinstance(obj, integer_types) or \
            isinstance(obj, float) or \
            obj is None or \
            isinstance(obj, yson.YsonType) or \
            isinstance(obj, (binary_type, text_type)):
        return obj
    elif isinstance(obj, list):
        return [_filter_simple_types(item) for item in obj]
    elif isinstance(obj, collections.Mapping):
        return dict([(key, _filter_simple_types(value)) for key, value in iteritems(obj)])
    return None

def get_environment_for_binary_test():
    env = {
        "PYTHONPATH": os.environ["PYTHONPATH"],
        "PYTHON_BINARY": sys.executable,
        "YT_USE_TOKEN": "0",
        "YT_VERSION": yt.config["api_version"],
        "YT_PRINT_BACKTRACE": "1",
    }
    env["YT_CONFIG_PATCHES"] = yson._dumps_to_native_str(_filter_simple_types(yt.config.config))
    return env

def build_python_egg(egg_contents_dir, temp_dir=None):
    dir_ = tempfile.mkdtemp(dir=temp_dir)

    for obj in os.listdir(egg_contents_dir):
        src = os.path.join(egg_contents_dir, obj)
        dst = os.path.join(dir_, obj)
        if os.path.isdir(src):
            shutil.copytree(src, dst)
        else:  # file
            shutil.copy2(src, dst)

    _, egg_filename = tempfile.mkstemp(dir=temp_dir, suffix=".egg")
    try:
        subprocess.check_call(["python", "setup.py", "bdist_egg"], cwd=dir_)

        eggs = glob.glob(os.path.join(dir_, "dist", "*.egg"))
        assert len(eggs) == 1

        shutil.copy2(eggs[0], egg_filename)
        return egg_filename
    finally:
        shutil.rmtree(dir_, ignore_errors=True)

def dumps_yt_config():
    config = _filter_simple_types(yt.config.config)
    return yson._dumps_to_native_str(config)

def run_python_script_with_check(yt_env, script):
    dir_ = yt_env.env.path

    with tempfile.NamedTemporaryFile(mode="w", dir=dir_, suffix=".py", delete=False) as f:
        f.write(script)
        f.close()

        proc = subprocess.Popen([sys.executable, f.name], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out, err = proc.communicate(b(dumps_yt_config()))
        assert proc.returncode == 0, err

        return out, err

def wait(predicate):
    for _ in xrange(100):
        if predicate():
            return
        time.sleep(0.3)
    pytest.fail("wait failed")

def sync_create_cell():
    tablet_id = yt.create("tablet_cell", attributes={"size": 1})
    wait(lambda : yt.get("//sys/tablet_cells/{0}/@health".format(tablet_id)) == "good")

def wait_record_in_job_archive(operation_id, job_id):
    operation_id_hash_pair = yt.common.uuid_hash_pair(operation_id)
    job_id_hash_pair = yt.common.uuid_hash_pair(job_id)
    # Jobs
    key = {}
    key["operation_id_hi"], key["operation_id_lo"] = operation_id_hash_pair.hi, operation_id_hash_pair.lo
    key["job_id_hi"], key["job_id_lo"] = job_id_hash_pair.hi, job_id_hash_pair.lo
    wait(lambda: any(yt.lookup_rows("//sys/operations_archive/jobs", [key], column_names=["operation_id_hi"])))
    # Job specs
    key = {}
    key["job_id_hi"], key["job_id_lo"] = job_id_hash_pair.hi, job_id_hash_pair.lo
    wait(lambda: any(yt.lookup_rows("//sys/operations_archive/job_specs", [key], column_names=["spec_version"])))
