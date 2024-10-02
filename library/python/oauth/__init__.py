import getpass
import json
import logging
import requests
import time
import contextlib

import library.python.ssh_sign as lps

LOGGER = logging.getLogger(__name__)

ILLEGAL_USERS = set(
    [
        'root',
        'sandbox',
    ]
)


def get_token(client_id, client_secret, login=None, keys=None, device_id=None, device_name=None, raise_errors=False,
              query_passwd=True, session=None):
    if login is None:
        login = getpass.getuser()

    if login in ILLEGAL_USERS:
        if raise_errors:
            raise Error('No SSH keys could be found for ' + login)
        else:
            return

    ts = int(time.time())
    string_to_sign = '%s%s%s' % (ts, client_id, login)

    data = {
        'grant_type': 'ssh_key',
        'client_id': client_id,
        'client_secret': client_secret,
        'login': login,
        'ts': ts,
    }

    if device_id:
        data['device_id'] = device_id
        if device_name:
            data['device_name'] = device_name

    oauth_error = None

    for sign in lps.sign(string_to_sign, keys=keys, query_passwd=query_passwd):
        key_data = json.loads(json.dumps(data))
        key_data['ssh_sign'] = sign

        if 'cert' in sign.key_type:
            key_data['public_cert'] = sign.pub_key

        with _make_session(session) as s:
            r = s.request(method='post',
                          url='https://oauth.yandex-team.ru/token',
                          data=key_data)

        if r.ok:
            return r.json()['access_token']
        else:
            oauth_error = _make_oauth_error(r)

    if raise_errors:
        if oauth_error:
            raise oauth_error
        else:
            raise Error('No SSH keys found')


def get_token_by_password(client_id, client_secret, login=None, password=None, raise_errors=True, session=None):
    if login is None:
        login = getpass.getuser()
    if password is None:
        password = getpass.getpass()

    with _make_session(session) as s:
        r = s.request(
            method='post',
            url='https://oauth.yandex-team.ru/token',
            data={
                'grant_type': 'password',
                'client_id': client_id,
                'client_secret': client_secret,
                'username': login,
                'password': password,
            },
        )

    if r.ok:
        return r.json()['access_token']
    else:
        oauth_error = _make_oauth_error(r)
        if raise_errors:
            raise oauth_error


class Error(Exception):
    pass


class OAuthError(Error):
    def __init__(self, status_code, error=None, error_description=None):
        msg = 'OAuth error \'{}\' (http code {})'.format(error or 'unknown', status_code)
        if error_description:
            msg += ': {}'.format(error_description)
        super(OAuthError, self).__init__(msg)
        self.status_code = status_code
        self.error = error
        self.error_description = error_description


def _make_oauth_error(r):
    try:
        error_data = r.json()
    except Exception:
        error = OAuthError(r.status_code)
    else:
        error = OAuthError(r.status_code, error_data.get('error'), error_data.get('error_description'))
    LOGGER.debug('Token request failed: %s', error)
    return error


@contextlib.contextmanager
def _make_session(s=None):
    local_session = s is None
    if local_session:
        s = requests.Session()

    try:
        yield s
    finally:
        if local_session:
            s.close()
