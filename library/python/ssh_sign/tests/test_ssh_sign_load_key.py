# -*- coding: utf-8 -*-

from library.python.ssh_sign import load_rsa_from_file, load_dsa_from_file
from paramiko import pkey


# RSA key generated with `ssh-keygen -t rsa -m PEM -N "" -f id_rsa`
# as described on https://wiki.yandex-team.ru/doc-and-loc/doc/newbies/mac/ssh-authentication-keys/
RSA_PRIVATE_TEST_KEY = """-----BEGIN RSA PRIVATE KEY-----
MIIEowIBAAKCAQEAt0ugb55eLJ6TE1tIAox2GFmKc8++Zu7vKEsSbKyk6Kr1RoHm
faGwidJ7JWfkTLCtqEBgkIfNxrUot49+z2wIZWTaDRtgjwgGbHycPXNePOR4Gsoz
2M1xR/o79KQR5l43FRRQ7OB6n8Cyb702W9xvcboM8Y3zdCHEIz8nheNGW7OZfp/n
h1J2NizHEEawd0UAuc67Z2DFJoQfyf87SpwDVLv9hWo/br6EkFMk0Jvq2YwFNPlK
9AvluJgjs0Mi1Dzs8PS5apFGKvz/1A0C3yCU0ZsbcRAfKqEodIs0hzsoLEn50Pcw
Izxs8Rm0vSzl2na9sBCDeSdX0f6lagFiYc1sVQIDAQABAoIBAF9UaPPEdQxVyP6H
OhOBRCZz42tKP6e/WwkkykdVH0uXJovuIZdzkaKMotmYiAdO2HKqUM66os5XB/84
FWcBs/gwFKw+ceDR9q1TvdrD9t5KoSCly4NVjjY2MhqRfnHGzHUdJN2d/oi5qtQs
n2n1YszOS88R2e2rZhxCy2Z6BuzHwLtwbtqT56OWz7qsFPUMAVSfLe+0R/IKnoYK
BWnrMgaqo1xA3JiflMFsRzgp0pvORhK/LK4fMnTPgaTmBUqmy18OR4yVRKmrtvC3
OD+MrrBKmmWe3zFPXsA95jdSZZTXXlvVQawmM8pOvQN/zDKeZL9BvJ3Y6H1qO4WE
KrcXKaECgYEA7HUDatSJW8ZAI1Chb6hE/CJ40pmPEpBXgX9OFB4s1q0UAeX+8Y2u
8rp1lFAPQbt2Onhr/XITHNg//fa2HnpweIEj0uRQSMH/rkEr3f0jpD9pj726JfhX
4PJT0TzOvqQG8SEf/axVKvLNQeMwKfd5DjNxD8sV5iGbnP8TD9MDVH0CgYEAxnHQ
BduGaiqZCzvpD9xeiVRUUglPKlddueNug+hn55DL3Gq3IfnQhcX3GIYhKaCVUsyO
JfRYPZGrsya3WHMnZqQQB8R9BUJaG6L7J1cpd+95MxLHaluZGCoehjLQHdxfs1Wi
HZylLf8H2+5XrMtw0DLAi3BwfwSZIekibfZPNrkCgYB7yWJJmgxyrB3Fnx90ec3r
hQxljBhXapM06vVwGL/2ftNbjxFhWX/9+Fk3lJX4HnBteb9nBkI1NoyBopeC6yxY
GZsse3QAMvdsPRf+9dej08KqinOaDyHKVHJGtqOxL+OmdmXuAjrv6f5EgPAk+DY6
lfmZgALt4Cg8D1jmTtyObQKBgQCJo8c1SN4iJQmh0RwF9ENd3S/e9rYCGDbaB/VP
aJMo+jVr3FjJKAyJvJV2XRIDXW40z8yIZsINckw1JlVk8/oQJxs3SAGu9CarpI+u
6bXJij/2PMAz9PRq3kvtqLDRBVkbefnHsC6hiJJa6SXGpBTLU2hZTtO4RegwrRNE
UeL/gQKBgFoeIBmxTnt9CpGuQ7WllCu6W/KiRZ6fQHcx0qI8NefdyvgyxR9FvorC
BiiCaqIVEIIgA8pD7AW6+3b0lRTrsmpCeIaQYPeAh3C7jB0YZQQzX2vclxgtGfpg
u4C7FZFLdXLk8pBMQY/iiiv+C2Nvm9LE8URCKP4ZLhwXdWXFinci
-----END RSA PRIVATE KEY-----
"""


# DSA key generated with `ssh-keygen -t dsa -m PEM -N "" -f id_dsa`
DSA_PRIVATE_TEST_KEY = """-----BEGIN DSA PRIVATE KEY-----
MIIBugIBAAKBgQC8HbtczdZv9CGXanhYk2O3HBDaWojSwFHVlUBYJvN3Y6SdJySQ
O5d+3WBY9fSAgaU4EH4Qqn2gW8gCIyBSXdGvl13OPvOnXbJKJv5VDv26tHNBCTNE
i4YM4Wd8l6P9o3asQkQs3syF/WYCbsm+uhFi+/64XTNI5WOERm+5HiUKGwIVAIQU
Eirawsn+ebRWHmeHpT0VbqKLAoGAUdUgKTRMDdm8cRREgz3V1XAfgrrZyjcOQHtR
b4zjDHWifAqGTfd/1tdC/HQ+gIRPP7RnpLkT1zdAxmnegkbuxkxkNdhtkgfXBtji
59yHVJ8+TuWx3HlUGNv+FGRxwj0u1gAfi018sKImM7ZG/o11rscsee3tw9EAgBvp
lbNPl2ACgYAnJ3A+TFhfjmrG+s032c3zXHwS228hFCTl4L+XyaXYC8EKq7WuFSaD
VvazP+q+GxKruAP8BvNgUhygCNGswJkY0AON/baMBuR4y44AT746XBQ1Lc7sBXp7
DDliKx6XTgIGgfP8kzJmqkpRPySfhduR5ZXZSgrYtm0NVoHlwxsPcgIUfiFiHdDm
qwXDxhJvhqkUrjsljGM=
-----END DSA PRIVATE KEY-----
"""


def _test_key(key_value, key_path, loading_func, encode_as_bytes=False):
    with open(key_path, 'w') as f:
        f.write(key_value)
    if encode_as_bytes:
        key_path = key_path.encode()
    key = loading_func(key_path)
    assert isinstance(key, pkey.PKey)


def test_load_rsa_key_str():
    _test_key(RSA_PRIVATE_TEST_KEY, 'id_rsa', load_rsa_from_file)


def test_load_rsa_key_bytes():
    _test_key(RSA_PRIVATE_TEST_KEY, 'id_rsa', load_rsa_from_file, True)


def test_load_dsa_key_str():
    _test_key(DSA_PRIVATE_TEST_KEY, 'id_dsa', load_dsa_from_file)


def test_load_dsa_key_bytes():
    _test_key(DSA_PRIVATE_TEST_KEY, 'id_dsa', load_dsa_from_file, True)
