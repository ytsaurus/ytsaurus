import os
import six
import sys
import base64
import re

from library.python import strings


TOKEN_PREFIX = 'AQAD-'
PRIVATE_KEY_PREFIX_RE = re.compile(r"-----BEGIN (RSA |EC |DSA )?PRIVATE KEY-----")
IN_CHECKS = ('access_key',)
ENDSWITH_CHECKS = (
    'token',
    'secret',
    'password',
)


def may_be_token(key, value):
    if not value:
        return False
    if not isinstance(value, six.string_types):
        return False

    if TOKEN_PREFIX in value:
        return True

    _key_lower = key.lower()

    if _key_lower.endswith(ENDSWITH_CHECKS) or any((key_part in _key_lower for key_part in IN_CHECKS)):
        try:
            if six.ensure_text(value).isdecimal() and len(value) < 10:
                # We treat short enough (10^10 ~= 2^30) numbers as non-secrets
                # This should be enough to avoid reasonable numbers in variables
                return False
        except BaseException:
            pass
        return True

    if PRIVATE_KEY_PREFIX_RE.search(value):
        return True

    # detect aws keys
    try:
        if b"aws" in base64.b64decode(value):
            return True
    except Exception:
        pass

    return False


def environ(env=None):
    def extract(var, value):
        if may_be_token(var, value):
            return '[SECRET]'
        return value

    return dict((k, extract(k, v)) for k, v in six.iteritems(env or os.environ))


def mine_env_vars():
    def match(var):
        if var.startswith('YA_'):
            return True
        if var in ['TOOLCHAIN', 'CC', 'CXX', 'USER']:
            return True
        return False

    def extract(var, value):
        if may_be_token(var, value):
            return '[SECRET]'
        return value

    return dict((k, extract(k, v)) for k, v in six.iteritems(os.environ) if match(k))


def mine_cmd_args():
    return [strings.to_unicode(arg, strings.guess_default_encoding()) for arg in sys.argv]


def iter_argv_and_env():
    for x in sys.argv:
        if x.startswith("--"):
            for y in x.split("=", 1):
                yield y
        else:
            yield x

    for k, v in six.iteritems(os.environ):
        yield k
        yield v


def mine_suppression_filter():
    secrets = set()
    key = 'dummy'
    for value in iter_argv_and_env():
        if may_be_token(key, value):
            secrets.add(value)
        key = value

    return sorted(list(secrets))
