import warnings

warnings.filterwarnings(action='ignore', message='Python 2 is no longer supported')

from paramiko import pkey
from paramiko import agent
from paramiko import message
from paramiko import util
from paramiko import common
from paramiko import SSHException

import logging
import openssl
import os
import struct

from hashlib import sha1
from base64 import urlsafe_b64encode
import six


_LOGGER = logging.getLogger(__name__)


class Signature(object):
    """
    Representation of SSH signature.
    """

    __slots__ = ['key_type', 'pub_key', 'sign']

    def __init__(self, key, sign):
        self.key_type = key.get_name()
        self.pub_key = b64u_encode(key.asbytes())
        self.sign = b64u_encode(sign)

    def __str__(self):
        return six.ensure_text(self.sign)

    def __repr__(self):
        return 'SSH Signature {!r} of key of type {!r}'.format(self.sign, self.key_type)


def b64u_encode(s):
    return urlsafe_b64encode(s).rstrip(b'=')


def load_rsa_from_file(path, query_passwd=True):
    class RSAKey(pkey.PKey):
        """
        Representation of an RSA key which can be used to sign and verify SSH2
        data.
        """

        def __init__(self, key):
            self._key = key

        def asbytes(self):
            m = message.Message()
            m.add_string('ssh-rsa')
            m.add_mpint(self._key.get_e())
            m.add_mpint(self._key.get_n())
            return m.asbytes()

        def get_name(self):
            return 'ssh-rsa'

        def sign_ssh_data(self, data):
            digest = sha1(data).digest()
            sig = self._key.sign(digest)
            m = message.Message()
            m.add_string('ssh-rsa')
            m.add_string(sig)
            return m.asbytes()

    try:
        if not isinstance(path, bytes):
            path = path.encode()
        return RSAKey(openssl.PrivateRsaKey(path, query_passwd=query_passwd))
    except Exception as e:
        _LOGGER.debug("Could not load rsa key from {}: {}".format(path, str(e)))
    return None


def load_dsa_from_file(path, query_passwd=True):
    class DSAKey(pkey.PKey):
        """
        Representation of an RSA key which can be used to sign and verify SSH2
        data.
        """

        def __init__(self, key):
            self._key = key

        def asbytes(self):
            m = message.Message()
            m.add_string('ssh-dss')
            m.add_mpint(self._key.get_p())
            m.add_mpint(self._key.get_q())
            m.add_mpint(self._key.get_g())
            m.add_mpint(self._key.get_y())
            return m.asbytes()

        def get_name(self):
            return 'ssh-dss'

        def sign_ssh_data(self, data):
            digest = sha1(data).digest()
            r, s = self._key.sign(digest)
            rstr = util.deflate_long(r, 0)
            sstr = util.deflate_long(s, 0)
            if len(rstr) < 20:
                rstr += common.zero_byte * (20 - len(rstr))
            if len(sstr) < 20:
                sstr += common.zero_byte * (20 - len(sstr))

            m = message.Message()
            m.add_string('ssh-dss')
            m.add_string(rstr + sstr)
            return m.asbytes()

    try:
        path = six.ensure_binary(path)
        return DSAKey(openssl.PrivateDsaKey(path, query_passwd=query_passwd))
    except Exception as e:
        _LOGGER.debug("Could not load dsa key from {}: {}".format(six.ensure_str(path), str(e)))

    return None


def sign(data, keys=None, query_passwd=True):
    def _get_token_using_ssh_keys():
        def load_rsa(rsa_path):
            if os.path.exists(rsa_path):
                _LOGGER.debug('trying to load RSA key from %s', rsa_path)
                k = load_rsa_from_file(rsa_path, query_passwd=query_passwd)

                if k:
                    yield "id_rsa", k
            else:
                _LOGGER.debug('%s doesn\'t exist', rsa_path)

        def load_dsa(dsa_path):
            if os.path.exists(dsa_path):
                _LOGGER.debug('trying to load DSA key from %s', dsa_path)
                k = load_dsa_from_file(dsa_path, query_passwd=query_passwd)

                if k:
                    yield "id_dsa", k
            else:
                _LOGGER.debug('%s doesn\'t exist', dsa_path)

        def load_from_agent():
            try:
                _LOGGER.debug('trying to load key from SSH agent')
                a = agent.Agent()
                try:
                    for k in a.get_keys():
                        yield "agent key", k
                finally:
                    a.close()
            except Exception as e:
                _LOGGER.warning("could not get keys from agent: %s", e)

        if keys:
            for key in keys:
                for x in load_rsa(key):
                    yield x

                for x in load_dsa(key):
                    yield x
        else:
            # try agent first, cause .ssh key can have password protection
            for x in load_from_agent():
                yield x

            # try common places
            for x in load_rsa(os.path.realpath(os.path.expanduser(os.path.join("~", ".ssh", "id_rsa")))):
                yield x

            for x in load_dsa(os.path.realpath(os.path.expanduser(os.path.join("~", ".ssh", "id_dsa")))):
                yield x

    for x, key in _get_token_using_ssh_keys():
        try:
            if not isinstance(data, bytes):
                data = data.encode()
            d = bytes(key.sign_ssh_data(data))
            parts = []

            while d:
                len = struct.unpack('>I', d[:4])[0]
                bits = d[4 : len + 4]
                parts.append(bits)
                d = d[len + 4 :]

            yield Signature(key, parts[1])
        except SSHException as exc:
            _LOGGER.error('Unable to sign with %s key: %r', x, exc)
