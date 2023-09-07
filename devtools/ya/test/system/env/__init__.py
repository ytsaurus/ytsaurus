# coding: utf-8

import os
import six

from test import const
import library.python.func
import devtools.ya.core.sec as sec


# TODO get rid
def extend_env_var(env, name, value, sep=":"):
    return sep.join([_f for _f in [env.get(name), value] if _f])


class Environ(object):
    PATHSEP = ':'

    def __init__(self, env=None, only_mandatory_env=False):
        env = os.environ.copy() if env is None else env
        if only_mandatory_env:
            self._env = {}
        else:
            self._env = env

        self._mandatory = set()
        names = [_f for _f in self._env.get(const.MANDATORY_ENV_VAR_NAME, '').split(self.PATHSEP) if _f]
        for name in names:
            self._mandatory.add(name)
            self._env[name] = env[name]

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        del self._env[key]
        if key in self._mandatory:
            self._mandatory.remove(key)

    def __getitem__(self, item):
        return self._env[item]

    def __contains__(self, item):
        return item in self._env

    def __iter__(self):
        return iter(self._env)

    def get(self, name, default=None):
        if default is None:
            return self[name]
        return self._env.get(name, default)

    def set(self, name, value):
        self._env[name] = value

    def set_mandatory(self, name, value):
        self._env[name] = value
        self._mandatory.add(name)

    def items(self):
        return self._env.items()

    def update(self, data):
        self._env.update(data)

    def update_mandatory(self, data):
        self._env.update(data)
        self._mandatory.update(data.keys())

    def _extend(self, name, val):
        if name in self._env:
            prefix = self.get(name, '')
        else:
            prefix = os.environ.get(name)

        if prefix:
            return prefix + self.PATHSEP + val
        return val

    def extend(self, name, val):
        self.set(name, self._extend(name, val))

    def extend_mandatory(self, name, val):
        self.set_mandatory(name, self._extend(name, val))

    def adopt_mandatory(self, name):
        if name in os.environ:
            self._env[name] = os.environ[name]
        self._mandatory.add(name)

    def adopt_update_mandatory(self, names):
        for x in names:
            self.adopt_mandatory(x)

    def dump(self, safe=False):
        env = dict(self._env)

        if safe:
            env = sec.environ(env)

        if self._mandatory:
            entries = sorted(set([const.MANDATORY_ENV_VAR_NAME] + list(self._mandatory)))
            env[const.MANDATORY_ENV_VAR_NAME] = self.PATHSEP.join(entries)
        for k, v in env.items():
            assert isinstance(v, six.string_types), (k, v)
        return env


@library.python.func.lazy
def get_common_env():
    env_vars = [name for name in ["SVN_SSH"] if name in os.environ]
    # Work in progress, see YA-365
    skip = (
        "YA_CACHE_DIR",
        "YA_TIMEOUT",
    )
    env_vars += [name for name in os.environ if name.startswith("YA_") and not name.startswith(skip)]
    return {name: os.environ.get(name) for name in env_vars}


def get_common_py_env():
    env = Environ(
        {
            "PY_IGNORE_ENVIRONMENT": "",
            # pytest installs own import hook to overwrite asserts - AssertionRewritingHook
            # Tests can import modules specified in the DATA which will generate patched pyc-files.
            # We are setting PYTHONDONTWRITEBYTECODE=1 to prevent this behaviour.
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    env.update(get_common_env())
    return env


def update_test_initial_env_vars(env, suite, opts):
    env.update(
        {
            "ARCADIA_BUILD_ROOT": "$(BUILD_ROOT)",
            "ARCADIA_ROOT_DISTBUILD": "$(SOURCE_ROOT)",
            "ARCADIA_SOURCE_ROOT": "$(SOURCE_ROOT)",
            "TEST_NAME": suite.name,
            "YA_TEST_RUNNER": "1",
            "GORACE": "halt_on_error=1",
        }
    )

    if suite.get_atd_data():
        env["ARCADIA_TESTS_DATA_DIR"] = "$(TESTS_DATA_ROOT)"

    for san_opts in "LSAN_OPTIONS", "ASAN_OPTIONS", "UBSAN_OPTIONS", "MSAN_OPTIONS":
        env.extend_mandatory(san_opts, "exitcode={}".format(const.SANITIZER_ERROR_RC))
    env.extend_mandatory("UBSAN_OPTIONS", "print_stacktrace=1,halt_on_error=1")

    suite.setup_environment(env, opts)
