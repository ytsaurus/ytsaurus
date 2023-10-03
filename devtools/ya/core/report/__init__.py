import os
import sys
import logging
import platform
import getpass
import uuid
import time
import json

from exts import func
from exts import flatten
from exts import strings
from core import config
from core import gsid


logger = logging.getLogger(__name__)
SUPPRESSIONS = None


class ReportTypes(object):
    EXECUTION = 'execution'
    RUN_YMAKE = 'run_ymake'
    FAILURE = 'failure'
    TIMEIT = 'timeit'
    LOCAL_YMAKE = 'local_ymake'
    HANDLER = 'handler'
    DIAGNOSTICS = 'diagnostics'
    WTF_ERRORS = 'wtf_errors'
    PROFILE_BY_TYPE = 'profile_by_type'
    PLATFORMS = 'platforms'
    VCS_INFO = 'vcs_info'
    PARAMETERS = 'parameters'
    YT_CACHE_ERROR = 'yt_cache_error'
    GRAPH_STATISTICS = 'graph_statistics'


@func.lazy
def default_namespace():
    return 'yatool' + ('-dev' if config.is_developer_ya_version() else '')


@func.lazy
def get_distribution():
    if sys.version_info > (3, 7):
        import distro
        linux_distribution = '{} {} {}'.format(distro.name(), distro.version(), distro.codename()).strip()
    else:
        linux_distribution = ' '.join(platform.linux_distribution()).strip()
    windows_distribution = ' '.join(platform.win32_ver()).strip()
    mac_distribution = ' '.join(flatten.flatten(platform.mac_ver())).strip()
    return linux_distribution + windows_distribution + mac_distribution


class ReportEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, set):
                obj_to_send = list(obj)
            else:
                obj_to_send = str(obj)
        except Exception:
            logger.exception("While converting %s", repr(obj))
            return super(ReportEncoder, self).default(obj)

        logger.debug(
            "Convert %s (%s) to `%s` (%s)",
            repr(obj), type(obj),
            repr(obj_to_send), type(obj_to_send),
        )

        return obj_to_send


@func.lazy
def system_info():
    return platform.system() + ' ' + platform.release() + ' ' + get_distribution()


def set_suppression_filter(suppressions):
    global SUPPRESSIONS
    SUPPRESSIONS = suppressions


class CompositeTelemetry:
    def __init__(self, backends={}):
        self.backends = backends

    def report(self, key, value, namespace=default_namespace()):
        if not self.backends:
            return

        def __filter(s):
            if SUPPRESSIONS:
                for sup in SUPPRESSIONS:
                    s = s.replace(sup, '[SECRET]')
                return s
            return s

        try:
            value = strings.unicodize_deep(value)
            svalue = json.loads(__filter(json.dumps(value, cls=ReportEncoder)))
        except Exception as e:
            # Don't expose exception: it may contain secret
            svalue = 'Unable to filter report value: {}'.format(e)

        logger.debug('Report %s: %s', key, svalue)
        for telemetry in self.backends.values():
            telemetry.push({
                '_id': uuid.uuid4().hex,
                'hostname': platform.node(),
                'user': getpass.getuser(),
                'platform_name': system_info(),
                'session_id': gsid.session_id(),
                'namespace': namespace,
                'key': key,
                'value': svalue,
                'timestamp': int(time.time()),
            })
        logger.debug('Reporting done')

    def init_reporter(self, shard='report', suppressions=None):
        if not self.backends:
            return

        for telemetry_name, telemetry in self.backends.items():
            telemetry.init(os.path.join(config.misc_root(), telemetry_name), shard)
        global SUPPRESSIONS
        SUPPRESSIONS = suppressions

    def request(self, tail, data, backend='snowden'):
        telemetry = self.backends.get(backend, None)
        if telemetry:
            return telemetry.request(tail, data)
        return "{}"


def gen_reporter():
    backends = {}

    try:
        from yalibrary import snowden
        backends['snowden'] = snowden
    except ImportError:
        pass

    return CompositeTelemetry(backends)


telemetry = gen_reporter()
