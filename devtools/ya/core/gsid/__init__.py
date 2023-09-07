import os

import core.config
import exts.func
import exts.uniq_id


@exts.func.lazy
def _get_env_session():
    return os.environ.get('GSID', '').split()


@exts.func.lazy
def uid():
    return exts.uniq_id.gen16()


@exts.func.lazy
def _get_current_session():
    ya_name = 'YA-DEV' if core.config.is_developer_ya_version() else 'YA'
    return [ya_name + ':' + uid()]


@exts.func.lazy
def _get_user_session():
    return ['USER' + ':' + core.config.get_user()]


@exts.func.lazy
def session_id():
    return _get_env_session() + _get_user_session() + _get_current_session()


@exts.func.lazy
def flat_session_id():
    return ' '.join(session_id())
