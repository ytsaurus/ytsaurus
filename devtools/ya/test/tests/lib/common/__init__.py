import copy
import exts.yjson as json
import fnmatch
import inspect
import logging
import multiprocessing as mp
import os
import pytest
import random
import shutil
import six
import sys
import tempfile
import time

from six.moves import filter, map

import yatest.common
import exts.windows
import exts.archive
import exts.fs
import exts.os2
import xml.dom.minidom as xml_parser
from core import config, respawn, error
from devtools.ya.test.programs.test_tool.lib import monitor
from yalibrary import display
from yatest.common import process
from test.const import Status
import test.tests.test_tools as test_tools

logger = logging.getLogger("test.tests.common")

skip_on_win = pytest.mark.skipif(exts.windows.on_win(), reason="test is not supposed to run on windows")
on_win_only = pytest.mark.skipif(not exts.windows.on_win(), reason="test is supposed to run on windows only")


class RunYaResult(object):
    def __init__(self, out, err, exit_code, elapsed):
        self.exit_code = exit_code
        if out is None:
            self.out = None
        else:
            self.out = six.ensure_str(out)
        if err is None:
            self.err = None
        else:
            self.err = six.ensure_str(err)
        self.elapsed = elapsed


def run_ya(
        command,
        build_root=None, cwd=None, use_ya_bin=True, ya_cache_dir=None,
        env=None, stdin=None, keep_env_keys=None, verbose=True,
        restart_on_error=True, no_respawn=False, fake_root=False, source_root=None, use_ymake_bin=False, strace=False,
        timeout=None, stderr=None, encoding=None, use_python=None, no_precise=False, stdout=None,
):
    """
    runs ya-bin
    :param command: ya command name
    :param cwd: ya cwd
    :return: out, err or throws subprocess.CalledProcessError if the command return code is not 0.
    """

    if yatest.common.get_param("acceptance", False):
        no_respawn = True
        use_ya_bin = False

    if exts.windows.on_win():
        use_ya_bin = False

    if not env:
        env = os.environ.copy()

    if use_ya_bin:
        if use_python is None or use_python == 2:
            path_to_ya_bin = os.path.join('devtools', 'ya', 'bin', 'ya-bin')
        elif use_python == 3:
            path_to_ya_bin = os.path.join('devtools', 'ya', 'bin3', 'ya-bin')
        else:
            raise NotImplementedError("Unknown based python version: {}".format(use_python))
        ya = yatest.common.binary_path(path_to_ya_bin)
        is_ya_script = False
    elif exts.windows.on_win():
        ya = yatest.common.source_path("ya.bat")
        is_ya_script = True
    else:
        ya = [yatest.common.python_path(), yatest.common.source_path("ya")]
        is_ya_script = True

    if not isinstance(command, list):
        command = [command]

    if not isinstance(ya, list):
        ya = [ya]

    if strace:
        cmd = ['/usr/bin/strace'] + ya
    else:
        cmd = ya

    if use_python and is_ya_script:
        cmd += ["-{}".format(use_python)]

    if verbose:
        if no_precise:
            cmd += ['-v']
        else:
            cmd += ['--precise']
    cmd += command
    cwd = cwd or os.getcwd()
    keep_env_keys = copy.deepcopy(keep_env_keys) or []
    keep_env_keys.extend(['YA_CUSTOM_FETCHER', 'YA_TOKEN', 'YA_TOKEN_PATH', 'YA_TEST_CONFIG_PATH'])
    for key in list(six.viewkeys(env)):
        if key.startswith("YA") and key not in keep_env_keys:
            logging.debug("Remove envronment key `%s` from env", key)
            del env[key]

    # reuse tools dir if exists
    if "YA_CACHE_DIR_TOOLS" not in env:
        try:
            default_tool_root = os.path.dirname(config.tool_root())
            if os.path.exists(default_tool_root):
                logging.debug("Using existing tool root %s", default_tool_root)
                # devtools/ya/core/config/__init__.py, avoid redundant v3 parts.
                env["YA_CACHE_DIR_TOOLS"] = default_tool_root
            else:
                logging.debug("Tool root does not exist by %s", default_tool_root)
        except Exception as e:
            logging.debug("Cannot set default tool root: %s", e)

    ya_temp_cache_dir = None
    if not ya_cache_dir and not exts.windows.on_win():
        ya_temp_cache_dir = tempfile.mkdtemp()
        os.chmod(ya_temp_cache_dir, 0o755)
        ya_cache_dir = ya_temp_cache_dir
        env['YA_NO_LOGS'] = '1'
    if ya_cache_dir:
        env["YA_CACHE_DIR"] = ya_cache_dir

    # Treat some warnings as errors during testing stage
    env["YA_TEST_MODE"] = "1"
    # We don't want junk options with be taken into account
    env["YA_LOAD_USER_CONF"] = "0"
    env["ANSI_COLORS_DISABLED"] = "1"
    if "DO_NOT_USE_LOCAL_CONF" in env:
        del env["DO_NOT_USE_LOCAL_CONF"]

    if no_respawn and "YA_NO_RESPAWN" not in env:
        env["YA_NO_RESPAWN"] = "1"
        keep_env_keys.append("YA_NO_RESPAWN")

    if source_root:
        env["YA_SOURCE_ROOT"] = source_root
        keep_env_keys.append("YA_SOURCE_ROOT")

    if 'YA_TC' not in env:
        env['YA_TC'] = '1'
    if 'YA_AC' not in env:
        env['YA_AC'] = '1'

    env['YA_YT_STORE4'] = '0'

    # run integration tests with ymake from trunk - don't forget to add devtools/ymake/bin/ymake to DEPENDS section
    if use_ymake_bin and not yatest.common.get_param("acceptance", False):
        cmd += ["--ymake-bin", yatest.common.binary_path('devtools/ymake/bin/ymake')]

    env = respawn.filter_env(env)
    logger.info("Will run %s in %s with env %s", cmd, cwd, env)
    cmd = list(map(str, cmd))

    if fake_root:
        cmd = ["/usr/bin/fakeroot", "--"] + cmd

    if timeout:
        def on_timeout_func(exec_obj, secs):
            mon = monitor.MemProcessTreeMonitor(exec_obj.process.pid, cmdline_limit=9999)
            proc_tree_str = mon.poll().dumps_process_tree()
            logger.warn("ya has overrun %s secs timeout. Process tree before termination:\n%s\n", secs, proc_tree_str)
            return process.on_timeout_gen_coredump(exec_obj, secs)
    else:
        on_timeout_func = None

    res = process.execute(
        cmd,
        check_exit_code=False,
        cwd=cwd,
        env=env,
        timeout=timeout,
        on_timeout=on_timeout_func,
        stdin=stdin,
        stderr=stderr,
        stdout=stdout,
        preexec_fn=lambda: os.setpgid(os.getpid(), os.getpid()),  # run ya-bin in a new process group
    )
    if res.exit_code == error.ExitCodes.INFRASTRUCTURE_ERROR and restart_on_error:
        raise yatest.common.RestartTestException()
    if ya_temp_cache_dir:
        exts.fs.remove_tree_safe(ya_temp_cache_dir)

    def decode(s):
        return s if encoding is None else six.ensure_text(s, encoding=encoding)

    return RunYaResult(decode(res.std_out), decode(res.std_err), res.exit_code, res.elapsed)


def run_ya_clone(cwd, dst, ya_cache_dir=None, no_junk=False, revision=None,
                 apply_local_changes=False, env=None, branch=None, thin=False, use_python=None):
    clone_command = [
        "clone", dst
    ]
    if no_junk:
        clone_command.append('--no-junk')
    if revision:
        clone_command.append('--revision={}'.format(revision))
    if branch:
        clone_command.append('--branch={}'.format(branch))
    if thin:
        clone_command.append('--thin')

    res = run_ya(command=clone_command, ya_cache_dir=ya_cache_dir, cwd=cwd, env=env, use_python=use_python)

    if apply_local_changes:
        cloned_arcadia = dst
        local_ya_dir = yatest.common.source_path("devtools/ya")
        for root, dirs, files in os.walk(local_ya_dir):
            root_clone_related = os.path.join(cloned_arcadia, "devtools", "ya", os.path.relpath(root, local_ya_dir))
            for dirname in ["."] + dirs:
                dirpath = os.path.normpath(os.path.join(root_clone_related, dirname))
                if not os.path.exists(dirpath):
                    os.mkdir(dirpath)
            for filename in files:
                if filename == "test-results":
                    continue
                src = os.path.join(root, filename)
                if os.path.islink(os.path.join(root, filename)):
                    continue
                shutil.copyfile(src, os.path.join(root_clone_related, filename))

    return res


def _load_results(resutls2_file):
    with open(resutls2_file) as f:
        return json.load(f)


def _get_current_test_file_name():
    test_name = yatest.common.context.test_name
    if exts.windows.on_win():
        test_name = test_name.rsplit("::")[-1]
    return test_tools.to_suitable_file_name(test_name)


class UnitTest(object):
    def binary_exists(self, *path):
        path = os.path.join(*path)
        if exts.windows.on_win():
            path += ".exe"
        return os.path.exists(path)


class RunYaMakeResult(RunYaResult):
    def __init__(self, out, err, exit_code, build_root, cache_root, output_root, results, elapsed):
        super(RunYaMakeResult, self).__init__(out, err, exit_code, elapsed)
        self.build_root = build_root
        self.cache_root = cache_root
        self.output_root = output_root
        self.results = results
        self._tests = []
        self._chunks = []
        self._configures = []
        self._process_results()
        self.id = random.random()

    def _process_results(self):
        if not self.results:
            return
        for r in self.results["results"]:
            if r["type"] in ["test", "style"]:
                self._tests.append(copy.deepcopy(r))
            elif r["type"] == "configure":
                self._configures.append(copy.deepcopy(r))

    def verify_test(
            self,
            test_name,
            subtest_name,
            status=None,
            error_type=None,
            comment=None,
            duration=None,
            tags=None,
            comment_contains=None,
            path=None,
            links=None,
            metrics=None,
            comment_doesnt_contain=None,
            result=None,
            toolchain=None,
            suite_name=None,
            chunk_name=None,
    ):
        subtest = self.get_test(test_name, subtest_name, path, toolchain=toolchain, suite_name=suite_name, chunk_name=chunk_name)

        if status:
            assert subtest["status"] == status, (subtest["status"], status)

        if error_type is not None:
            assert subtest["error_type"] == error_type, subtest["error_type"]

        snippet = display.strip_markup(subtest.get("rich-snippet", ""))
        if comment is not None:
            if inspect.isfunction(comment):
                comment(snippet)
            else:
                assert snippet == comment, (snippet, comment)

        if comment_contains is not None:
            if hasattr(comment_contains, '__iter__'):
                for comment in comment_contains:
                    assert comment in snippet, (comment, snippet)
            else:
                assert comment_contains in snippet, (comment_contains, snippet)

        if comment_doesnt_contain is not None:
            if not isinstance(comment_doesnt_contain, six.string_types) and hasattr(comment_doesnt_contain, '__iter__'):
                for comment in comment_doesnt_contain:
                    assert comment not in snippet, (comment, snippet)
            else:
                assert comment_doesnt_contain not in snippet, (comment_doesnt_contain, snippet)

        if duration is not None:
            if inspect.isfunction(duration):
                duration(subtest["duration"])
            # TODO remove and rewrite tests using duration for verification
            # else:
            #     assert subtest["duration"] * 0.9 < duration < subtest["duration"] * 1.1
        if tags is not None:
            if inspect.isfunction(tags):
                tags(subtest["tags"])
            else:
                assert set(subtest["tags"]) == set(tags), (set(subtest["tags"]), set(tags))
        if links is not None:
            if inspect.isfunction(links):
                links(subtest["links"])
            else:
                assert subtest["links"] == links, (subtest["links"], links)

        if metrics is not None:
            if inspect.isfunction(metrics):
                metrics(subtest["metrics"])
            else:
                assert subtest["metrics"] == metrics, (subtest["metrics"], metrics)

        if result is not None:
            if inspect.isfunction(result):
                result(subtest.get("result"))
            else:
                assert subtest.get("result") == result, (subtest.get("result"), result)

    def get_suites(self, suite_name):
        def flt(record):
            return record.get('suite', False) and record['name'] == suite_name

        found = list(filter(flt, self._tests))

        return found

    def get_test(self, test_name, subtest_name, path=None, toolchain=None, suite_name=None, chunk_name=None):
        lcls = locals()
        del lcls["self"]

        suite_ids = None
        if suite_name:
            suite_ids = tuple(record['id'] for record in self.get_suites(suite_name))

        chunk_ids = None
        if chunk_name:
            chunks = self.get_chunks(chunk_name)
            chunk_ids = tuple(record['id'] for record in chunks)
            assert len(chunk_ids) == 1, self.get_chunks()

        def flt(record):
            res = record["name"] == test_name and record.get("subtest_name", "") == subtest_name
            if path:
                res &= record["path"] == path
            if toolchain:
                res &= record["toolchain"] == toolchain
            if suite_ids:
                res &= record.get('suite_id', None) in suite_ids
            if chunk_ids:
                res &= record.get('chunk_id', None) in chunk_ids
            return res

        found = [t for t in self._tests if flt(t)]
        if not found:
            raise KeyError("{} was not found among: {}".format(lcls, ["{} - {}::{}({})".format(t["path"], t["name"], t.get("subtest_name", ""), t["toolchain"]) for t in self._tests]))
        if len(found) > 1:
            raise Exception("{} tests found for {}: {}".format(len(found), lcls, found))
        return found[0]

    def get_configure_error(self, path, toolchain=None):
        lcls = locals()
        del lcls["self"]

        def flt(record):
            res = record.get("path", None) == path
            if toolchain:
                res &= record["toolchain"] == toolchain
            return res

        found = [t for t in self._configures if flt(t)]
        if not found:
            raise KeyError("{} was not found among: {}".format(lcls, ["{} ({})".format(t.get("path", "<no path>"), t["toolchain"]) for t in self._configures]))
        if len(found) > 1:
            raise Exception("{} tests found for {}: {}".format(len(found), lcls, found))
        return found[0]

    def verify_configure_error(self, path, status, error_type=None, comment=None, comment_contains=None, comment_doesnt_contain=None):
        configure_error = self.get_configure_error(path)
        assert configure_error["status"] == status, (configure_error["status"], status)
        if error_type is not None:
            configure_error["error_type"] == error_type

        snippet = display.strip_markup(configure_error.get("rich-snippet", ""))
        if comment is not None:
            if inspect.isfunction(comment):
                comment(snippet)
            else:
                assert snippet == comment
        if comment_contains is not None:
            if hasattr(comment_contains, '__iter__'):
                for comment in comment_contains:
                    assert comment in snippet
            else:
                assert comment_contains in snippet

        if comment_doesnt_contain is not None:
            if hasattr(comment_doesnt_contain, '__iter__'):
                for comment in comment_doesnt_contain:
                    assert comment not in snippet
            else:
                assert comment_doesnt_contain not in snippet

    def suites_by_status(self, status, error_type=None):
        return self.entry_by_status(status, error_type, self.is_suite)

    def chunks_by_status(self, status, error_type=None):
        return self.entry_by_status(status, error_type, self.is_chunk)

    def tests_by_status(self, status, error_type=None):
        return self.entry_by_status(status, error_type, self.is_test)

    def entry_by_status(self, status, error_type, predicate):
        def flt(entry):
            res = entry.get("status") == status
            if error_type:
                res &= entry.get("error_type") == error_type
            return res

        return len([entry for entry in self._tests if predicate(entry) and flt(entry)])

    def is_chunk(self, entry):
        return entry.get('chunk')

    def is_suite(self, entry):
        return entry.get('suite')

    def is_test(self, entry):
        return not self.is_suite(entry) and not self.is_chunk(entry)

    def get_tests_count(self):
        return len([entry for entry in self._tests if self.is_test(entry)])

    def get_suites_count(self):
        return len([entry for entry in self._tests if self.is_suite(entry)])

    def get_chunks_count(self):
        return len([entry for entry in self._tests if self.is_chunk(entry)])

    def get_tests(self):
        return [x for x in self._tests if self.is_test(x)]

    def get_chunks(self, chunk_name=None):
        chunks = [x for x in self._tests if self.is_chunk(x)]
        # TODO DEVTOOLS-7609 waiting for CI-1217
        if not chunks:
            chunks = [x for x in self._tests if self.is_suite(x)]

        if chunk_name:
            if '*' in chunk_name:
                flt = lambda x: fnmatch.fnmatch(x['subtest_name'], chunk_name)
            else:
                flt = lambda x: x['subtest_name'] == chunk_name
            return list(filter(flt, chunks))
        return chunks


class YaMakeTest(UnitTest):
    CUSTOM_CACHE_DIR = os.environ.get("YA_TEST_CUSTOM_CACHE_DIR")
    CUSTOM_BUILD_DIR = os.environ.get("YA_TEST_CUSTOM_BUILD_DIR")
    CUSTOM_OUTPUT_DIR = None

    @classmethod
    def setup_class(cls):
        cls.build_dir = cls.CUSTOM_BUILD_DIR
        if yatest.common.get_param("shared_cache") == 'yes':
            cls.cache_dir = os.environ.get('YA_CACHE_DIR', os.path.expanduser('~/.ya'))
            cls.cleanup_cache = False
        else:
            cls.cache_dir = cls.CUSTOM_CACHE_DIR
            cls.cleanup_cache = True
        if not cls.cache_dir:
            if not exts.windows.on_win():
                cls.cache_dir = tempfile.mkdtemp()
                os.chmod(cls.cache_dir, 0o755)
        if not cls.build_dir:
            cls.build_dir = tempfile.mkdtemp()
            os.chmod(cls.build_dir, 0o755)

    @classmethod
    def teardown_class(cls):
        if cls.cleanup_cache:
            exts.fs.remove_tree_safe(cls.cache_dir)
        if not cls.CUSTOM_BUILD_DIR:
            exts.fs.remove_tree_safe(cls.build_dir)

    @classmethod
    def _run_build_command(cls, command, output_root=None, cwd=None, env=None, keep_env_keys=None, verbose=False,
                           restart_on_error=False, results_file=None, no_respawn=False, use_ya_bin=True, use_ymake_bin=True, stdin=None, strace=False,
                           no_links=False, timeout=None, stderr=None, create_output_root=True, ya_cache_dir=None, build_dir=None, encoding=None, no_precise=False,
                           use_python=None):
        test_name = yatest.common.context.test_name
        if exts.windows.on_win():
            test_name = test_name.rsplit("::")[-1]
        test_file_name = "{}_{}".format(test_tools.to_suitable_file_name(test_name), int(time.time()))
        load_results = False

        if not output_root and create_output_root:
            output_root = os.path.join(cls.build_dir, test_file_name, "out")
            command += ["--build-results-report", "results.json", "--build-report-type", "human_readable"]
            load_results = True

        command += ["--build-dir", build_dir or cls.build_dir]
        if create_output_root:
            command += ["--output", output_root]
            if "--evlog-file" not in command:
                command += ["--evlog-file", yatest.common.test_output_path('evlog.jsonl')]

        if no_links:
            command += ["--no-src-links"]
        else:
            command += ["--result-store-root", cls.build_dir]

        res = run_ya(
            command=command, cwd=cwd, ya_cache_dir=ya_cache_dir or cls.cache_dir, env=env,
            keep_env_keys=keep_env_keys, verbose=verbose, restart_on_error=restart_on_error, no_respawn=no_respawn,
            use_ya_bin=use_ya_bin, use_ymake_bin=use_ymake_bin, stdin=stdin, strace=strace, timeout=timeout,
            stderr=stderr, encoding=encoding, no_precise=no_precise, use_python=use_python,
        )

        if output_root and os.path.exists(output_root):
            paths = []
            for root, dirs, files in os.walk(output_root):
                for filename in files:
                    filename = os.path.join(root, filename)
                    # Skip binaries
                    if os.access(filename, os.X_OK):
                        logger.debug("Skipping %s ", filename)
                        continue
                    paths.append((filename, os.path.relpath(filename, output_root)))

            if paths:
                exts.archive.create_tar(paths, yatest.common.output_path(test_file_name + "_out.tar"))

        results = None
        if load_results:
            if not results_file:
                results_file = os.path.join(output_root, "results.json")
            if os.path.exists(results_file):
                results = _load_results(results_file)

        return RunYaMakeResult(res.out, res.err, res.exit_code, cls.build_dir, cls.cache_dir, output_root, results, res.elapsed)

    @classmethod
    def run_ya_make(cls, args=None, output_root=None, cwd=None, env=None, keep_env_keys=None,
                    verbose=False, targets=None, restart_on_error=False, profile=None, threads=4, no_respawn=False, use_ya_bin=True, use_ymake_bin=True, stdin=None, strace=False,
                    no_links=False, timeout=None, stderr=None, create_output_root=True, ya_cache_dir=None, build_dir=None, encoding=None, no_precise=False, use_python=None, custom_context=None):
        command = ["--no-report", "make", "--stat"]
        if profile:
            command += ["--build", profile]
        if output_root:
            command += ["--output", output_root]
        if threads is not None:
            command += ["--threads", threads]
        if custom_context:
            command += ["--custom-context", custom_context]
        for target in targets or []:
            command += ["--target", target]
        if args:
            command += args

        cwd = cwd or yatest.common.source_path()
        return cls._run_build_command(command, output_root, cwd, env, keep_env_keys, verbose, restart_on_error, no_respawn=no_respawn, use_ya_bin=use_ya_bin, use_ymake_bin=use_ymake_bin,
                                      stdin=stdin, strace=strace, no_links=no_links, timeout=timeout, stderr=stderr, create_output_root=create_output_root,
                                      ya_cache_dir=ya_cache_dir, build_dir=build_dir, encoding=encoding, no_precise=no_precise, use_python=use_python)


class YaTest(YaMakeTest):

    @classmethod
    def setup_class(cls):
        super(YaTest, cls).setup_class()
        cls._text = None
        cls._pos = 0

    @classmethod
    def run_ya_make_test(
        cls, cwd, args=None, retries=None, env=None, keep_env_keys=None, verbose=True,
        tags="ya:notags+ya:manual+ya:fat", restart_on_error=True, no_respawn=False, use_ya_bin=True, use_ymake_bin=True, strace=False, stdin=None, test_tool3=False, use_test_tool=True,
        create_output_root=True, no_links=False, ya_cache_dir=None, build_dir=None, retest=False, threads=4, use_python=None,
        timeout=None, no_src_changes=True,
    ):

        test_command_args = [
            "-t", "-P",
        ]
        if no_src_changes:
            test_command_args += ["--no-src-changes"]
        if retest:
            if args and "--cache-tests" in args:
                sys.stderr.write("You are running ya make -t with --retest and have specified a --cache-tests which will be suppressed\n")
            test_command_args += ["--retest"]
        if tags:
            test_command_args += ["--test-tag", tags]
        if test_command_args is None:
            test_command_args = []
        if retries:
            test_command_args += ["--tests-retries", str(retries)]
        if args:
            test_command_args += args

        # run integration tests with test_tool from trunk - don't forget to add devtools/ya/test/programs/test_tool to DEPENDS section
        if use_ya_bin and use_test_tool and not yatest.common.get_param("acceptance", False):
            test_command_args += ["--test-tool-bin", yatest.common.binary_path('devtools/ya/test/programs/test_tool/bin/test_tool')]
            # Automatically detect DEPENDS to test_tool3
            test_tool_bin3 = yatest.common.build_path('devtools/ya/test/programs/test_tool/bin3/test_tool3')
            if os.path.exists(test_tool_bin3):
                test_command_args += ["--test-tool3-bin", test_tool_bin3]
        return cls.run_ya_make(
            args=test_command_args, cwd=cwd, env=env, keep_env_keys=keep_env_keys, no_links=no_links,
            verbose=verbose, restart_on_error=restart_on_error, no_respawn=no_respawn, use_ya_bin=use_ya_bin, use_ymake_bin=use_ymake_bin, strace=strace, stdin=stdin,
            create_output_root=create_output_root, ya_cache_dir=ya_cache_dir, build_dir=build_dir, threads=threads, use_python=use_python,
            timeout=timeout,
        )

    @classmethod
    def run_ya_make_test_dist(cls, cwd=None, args=None, build_dir=None, use_python=None):
        if not yatest.common.get_param("rundist"):
            return pytest.skip("no dist test during the test session")
        test_args = [
            "--dist", "--dist-priority", "-3",
        ]
        if args:
            test_args += args

        return cls.run_ya_make_test(args=test_args, cwd=cwd, use_python=use_python)

    @classmethod
    def run_ya_autocheck(cls, cwd, args=None, mics_build_root=None, output_root=None, env=None, keep_env_keys=None, verbose=True, restart_on_error=True, use_ymake_bin=True, use_python=None):
        if not mics_build_root:
            mics_build_root = os.path.join(cls.build_dir, "mics_build_root")

        command = [
            "make", "--ignore-nodes-exit-code",
            "--warning-mode", ';'.join(['dirloops', 'ChkPeers', 'allbadrecurses']),
            "--stat", "-t", "--test-tag", "ya:notags+ya:manual"
        ] + (args or []) + [
            "--verbose", "--keep-going",
            "--misc-build-info-dir", mics_build_root,
            "--test-tool-bin", yatest.common.binary_path('devtools/ya/test/programs/test_tool/bin/test_tool'),
        ]
        test_tool3_bin = yatest.common.binary_path('devtools/ya/test/programs/test_tool/bin3/test_tool3')
        if os.path.exists(test_tool3_bin):
            command += [
                "--test-tool3-bin", test_tool3_bin,
            ]

        return cls._run_build_command(command, output_root, cwd, env, keep_env_keys, verbose, restart_on_error, results_file=os.path.join(mics_build_root, "results2.json"),
                                      use_ymake_bin=use_ymake_bin, no_links=True, use_python=use_python)

    @classmethod
    def run_ya_autocheck_dist(cls, cwd, mics_build_root=None, output_root=None, use_python=None):
        if not yatest.common.get_param("rundist"):
            return pytest.skip("no dist test during the test session")
        return cls.run_ya_autocheck(
            cwd, args=[
                "--dist", "--dist-priority", "-3",
                "--test-tag", "ya:notags+ya:manual"
            ], mics_build_root=mics_build_root, output_root=output_root, use_python=use_python
        )

    @classmethod
    def parse_junit(cls, junit_path, fill_suites=False):
        with open(junit_path) as junit:
            report = xml_parser.parseString(junit.read())

        tests = {}
        for suite_el in report.getElementsByTagName("testsuite"):
            subtests = {}
            for test_case_el in suite_el.getElementsByTagName("testcase"):
                test_name = test_case_el.attributes["name"].value

                if test_case_el.getElementsByTagName("failure"):
                    test_comment = test_case_el.getElementsByTagName("failure")[0].childNodes[0].wholeText
                    test_status = Status.FAIL
                elif test_case_el.getElementsByTagName("skipped"):
                    test_comment = test_case_el.getElementsByTagName("skipped")[0].childNodes[0].wholeText
                    test_status = Status.SKIPPED
                else:
                    test_comment = None
                    test_status = Status.GOOD

                if test_name in subtests:
                    raise Exception("Duplicating test '{}' found in suite '{}'".format(test_name, suite_el.attributes["name"].value))
                subtests[test_name] = {
                    "comment": test_comment,
                    "status": test_status
                }
            if fill_suites:
                suite_dict = {}
                for attr in ["failures", "name", "skipped", "tests", "time"]:
                    suite_dict[attr] = suite_el.attributes[attr].value
                subtests["suite"] = suite_dict
            tests[suite_el.attributes["name"].value] = subtests
        return tests

    def assert_find_next(self, msg, text=None):
        if text and text != self._text:
            self._text = text
            self._pos = 0
        pos = self._text.find(msg, self._pos)
        if pos == -1:
            raise AssertionError("Failed to find '{}'".format(msg))
        self._pos = pos


class YaJavaBuildTest(UnitTest):

    @classmethod
    def run_ya_jbuild_maven_export(cls, target, result_root, cwd=None, threads=None, tests=False, args=None, use_python=None):
        threads = threads or mp.cpu_count()
        args = args or []
        with exts.os2.change_dir(yatest.common.source_path()):
            command = [
                "make",
                target,
                '--maven-export',
                '--version', '1',
                '--threads', str(threads),
                '--results-root', result_root
            ]
            if tests:
                command += [
                    '--run-tests',
                    '--run-all-tests',
                    '--test-tool-bin',
                    yatest.common.binary_path('devtools/ya/test/programs/test_tool/bin/test_tool'),
                ]

            return run_ya(command + args, cwd, use_ymake_bin=True, use_python=use_python)

    @classmethod
    def run_ya_jbuild_java_dependency_tree(cls, target, result_root, cwd=None, threads=None, use_ymake_bin=True, no_respawn=False, use_python=None):
        threads = threads or mp.cpu_count()
        with exts.os2.change_dir(yatest.common.source_path()):
            command = [
                "java",
                "dependency-tree",
                target,
            ]
            return run_ya(command, cwd, use_ymake_bin=use_ymake_bin, no_respawn=no_respawn, use_python=use_python)


class RunYaPackageResult(RunYaResult):

    def __init__(self, run_ya_result, out_dir, source_root):
        super(RunYaPackageResult, self).__init__(run_ya_result.out, run_ya_result.err, run_ya_result.exit_code, run_ya_result.elapsed)
        self.output_root = out_dir
        self.source_root = source_root


def run_ya_package(packages, args=None, cwd=None, source_root=None, fake_root=False, use_ymake_bin=True, use_ya_bin=True, use_test_tool_bin=True, use_python=None, timeout=None):
    if not cwd:
        out_dir = yatest.common.path.get_unique_file_path(yatest.common.output_path(), _get_current_test_file_name(), create_file=False)
    else:
        out_dir = cwd
    logging.info("Package output dir: %s", out_dir)
    if args is None:
        args = []

    if use_test_tool_bin:
        # run integration tests with test_tool from trunk - don't forget to add devtools/ya/test/programs/test_tool to DEPENDS section
        args += ["--test-tool-bin", yatest.common.binary_path('devtools/ya/test/programs/test_tool/bin/test_tool')]

    arcadia_path = yatest.common.work_path("{}/arcadia".format(_get_current_test_file_name()))
    if not os.path.exists(arcadia_path):
        # ya package requires directory to be named exactly 'arcadia'
        exts.fs.ensure_dir(os.path.dirname(arcadia_path))
        exts.fs.symlink(yatest.common.source_path(), arcadia_path)
    command = [
        "package",
        "--no-cleanup", "--not-sign-debian",
    ] + args + packages

    if source_root is None and not source_root:
        command += ["--source-root", os.path.dirname(arcadia_path)]

    env = os.environ.copy()
    env['YA_SOURCE_ROOT'] = source_root or arcadia_path
    env['YA_NO_RESPAWN'] = "1"
    return RunYaPackageResult(run_ya(command, cwd=out_dir, keep_env_keys=['YA_SOURCE_ROOT', 'YA_NO_RESPAWN'], env=env,
                                     fake_root=fake_root, use_ymake_bin=use_ymake_bin, use_ya_bin=use_ya_bin, use_python=use_python, timeout=timeout), out_dir, arcadia_path)


class YaTestFuzz(YaTest):

    @classmethod
    def run_ya_make_fuzz(
        cls, cwd, args=None, retries=None, env=None, keep_env_keys=None, verbose=True,
        tags="ya:notags+ya:manual+ya:fat", restart_on_error=True, no_respawn=False, use_ya_bin=True, use_ymake_bin=True,
        corpus_filename=None, use_python=None,
    ):
        env = env or {}
        env = add_user_env_vars(env)
        if corpus_filename:
            env["YA_TEST_CORPUS_DATA_PATH"] = corpus_filename
            keep_env_keys = keep_env_keys or []
            keep_env_keys.append("YA_TEST_CORPUS_DATA_PATH")

        res = super(YaTestFuzz, cls).run_ya_make_test(
            cwd, args, retries, env, keep_env_keys, verbose, tags, restart_on_error, no_respawn,
            use_ya_bin, use_ymake_bin=use_ymake_bin, use_python=use_python,
        )
        return res


def set_svn_ssh_env(env, svn_login, svn_key_path):
    temp_dir = exts.tmp.temp_dir()
    if svn_key_path:
        svn_key_path_copy = os.path.join(temp_dir.path, "svn_key")
        exts.fs.copy_file(svn_key_path, svn_key_path_copy)
        os.chmod(svn_key_path_copy, 0o600)
        env['SVN_SSH'] = 'ssh -l {} -i {}'.format(svn_login, svn_key_path_copy)
    return temp_dir


class SegmentsMissing(Exception):
    pass


class CoverageYaTest(YaTest):

    @classmethod
    def setup_class(cls):
        YaTest.setup_class()
        cls._last_run_id = None
        cls._cov_data = {}

    def get_testdir(self, res):
        dirs = set()
        nonrelative = [
            # Binary path is outside of testing_out_stuff
            "binary",
        ]

        for subtest in res.get_chunks():
            if "links" in subtest:
                for name, val in subtest["links"].items():
                    if name not in nonrelative:
                        dirs.add(val[0])
        assert dirs, res.get_chunks()
        path = exts.fs.commonpath(dirs)
        if not os.path.exists(os.path.join(path, 'run_test.log')):
            raise Exception("Failed to find testdir: path={} dirs={}".format(path, dirs))
        return path

    def get_coverage(self, res, covtype, program_name=None):
        logger.debug("Getting coverage for run with %s id (current: %s covdata: %s)", res.id, self._last_run_id, self._cov_data.keys())
        if res.id != self._last_run_id:
            self._last_run_id = res.id
            self._cov_data = {}

        covkey = (covtype, program_name)

        if covkey not in self._cov_data:
            testdir = self.get_testdir(res)

            if covtype == 'cpp':
                assert os.path.exists(os.path.join(testdir, 'clangcov_resolve.done')), os.listdir(testdir)

            if program_name:
                filename = "coverage_resolved.{}.{}.json".format(covtype, program_name)
            else:
                filename = "coverage_resolved.{}.json".format(covtype)
            covjson = os.path.join(testdir, filename)
            assert os.path.exists(covjson), (covjson, os.listdir(testdir))

            data = []
            with open(covjson) as afile:
                for line in afile:
                    data.append(json.loads(line))
            assert data
            self._cov_data[covkey] = data

        return self._cov_data[covkey]

    def get_coverage_by_filename(self, res, filename, covtype, program_name=None):
        data = []
        covdata = self.get_coverage(res, covtype, program_name)
        for entry in covdata:
            if entry['filename'] == filename:
                data.append(entry['coverage'])
        if not data:
            raise SegmentsMissing()
        assert len(data) == 1, "Several coverage entries for the same filename found: {}".format(data)
        return data[0]

    def get_segments(self, res, filename, covtype, program_name=None):
        covdata = self.get_coverage_by_filename(res, filename, covtype, program_name)
        return covdata["segments"]

    def run_ya_coverage(self, dir, ok_tests, args=None, exit_code=0, test_tool3=False, use_python=None):
        args = args or []
        res = self.run_ya_make_test(cwd=dir, args=args, test_tool3=test_tool3, use_python=use_python)
        assert res.exit_code == exit_code, (res.exit_code, exit_code)
        assert res.tests_by_status("OK") == ok_tests, (res.tests_by_status("OK"), ok_tests)
        return res

    def get_unittest_bin_name(self, target_dir):
        arcdir = os.path.relpath(target_dir, yatest.common.source_path())
        return shorten_path(arcdir, max(0, 98 - len(arcdir))).replace("/", "-")


def add_user_env_vars(env):
    # Required for musl build (for more info see https://st.yandex-team.ru/DEVTOOLS-3838)
    # https://a.yandex-team.ru/arc/trunk/arcadia/contrib/tools/python/src/Lib/getpass.py?rev=971682#L151
    for name in ('LOGNAME', 'USER', 'LNAME', 'USERNAME'):
        if name in os.environ:
            env[name] = os.environ[name]
    return env


def shorten_path(path, limit):
    """
    Trim a/b/c/d to b/c/d, or c/d, or d, such that it is under limit or has no /.
    """
    if len(path) > limit:
        pos = path.find('/', len(path) - limit - 1)
        if pos == -1:
            pos = path.rfind('/')
        if pos != -1:
            return path[pos + 1:]
    return path
