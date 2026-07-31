# Copyright 2024 Confluent, Inc.
# Copyright 2023 Buf Technologies, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import typing
import uuid
from email.utils import parseaddr
from ipaddress import IPv4Address, IPv6Address, ip_address
from urllib import parse as urlparse

import celpy
from celpy import celtypes

from confluent_kafka.schema_registry.rules.cel import string_format


def _validate_hostname(host):
    if not host:
        return False
    if len(host) > 253:
        return False

    if host[-1] == ".":
        host = host[:-1]

    all_digits = True
    for part in host.split("."):
        if len(part) == 0 or len(part) > 63:
            return False

        # Host names cannot begin or end with hyphens
        if part[0] == "-" or part[-1] == "-":
            return False
        all_digits = True
        for r in part:
            if (r < "A" or r > "Z") and (r < "a" or r > "z") and (r < "0" or r > "9") and r != "-":
                return False
            all_digits = all_digits and "0" <= r <= "9"
    return not all_digits


def validate_email(addr):
    parts = parseaddr(addr)
    if addr != parts[1]:
        return False

    addr = parts[1]
    if len(addr) > 254:
        return False

    parts = addr.split("@")
    if len(parts) != 2:
        return False
    if len(parts[0]) > 64:
        return False
    return _validate_hostname(parts[1])


def validate_host_and_port(string: str, *, port_required: bool) -> bool:
    if not string:
        return False

    split_idx = string.rfind(":")
    if string[0] == "[":
        end = string.find("]")
        after_end = end + 1
        if after_end == len(string):  # no port
            return not port_required and validate_ip(string[1:end], 6)
        if after_end == split_idx:  # port
            return validate_ip(string[1:end]) and validate_port(string[split_idx + 1 :])
        return False  # malformed

    if split_idx == -1:
        return not port_required and (_validate_hostname(string) or validate_ip(string, 4))

    host = string[:split_idx]
    port = string[split_idx + 1 :]
    return (_validate_hostname(host) or validate_ip(host, 4)) and validate_port(port)


def validate_port(val: str) -> bool:
    try:
        port = int(val)
        return port <= 65535
    except ValueError:
        return False


def validate_ip(val: typing.Union[str, bytes], version: typing.Optional[int] = None) -> bool:
    try:
        if version is None:
            ip_address(val)
        elif version == 4:
            IPv4Address(val)
        elif version == 6:
            IPv6Address(val)
        else:
            msg = "invalid argument, expected 4 or 6"
            raise celpy.CELEvalError(msg)
        return True
    except ValueError:
        return False


def validate_uuid(val: str) -> bool:
    try:
        uuid.UUID(val)
        return True
    except ValueError:
        return False


def is_ipv4(val: celtypes.Value) -> celpy.Result:
    if not isinstance(val, (celtypes.BytesType, celtypes.StringType)):
        msg = "invalid argument, expected string or bytes"
        raise celpy.CELEvalError(msg)
    return celtypes.BoolType(validate_ip(val, 4))


def is_ipv6(val: celtypes.Value) -> celpy.Result:
    if not isinstance(val, (celtypes.BytesType, celtypes.StringType)):
        msg = "invalid argument, expected string or bytes"
        raise celpy.CELEvalError(msg)
    return celtypes.BoolType(validate_ip(val, 6))


def is_email(string: celtypes.Value) -> celpy.Result:
    if not isinstance(string, celtypes.StringType):
        msg = "invalid argument, expected string"
        raise celpy.CELEvalError(msg)
    return celtypes.BoolType(validate_email(string))


def is_uri(string: celtypes.Value) -> celpy.Result:
    if not isinstance(string, celtypes.StringType):
        msg = "invalid argument, expected string"
        raise celpy.CELEvalError(msg)
    url = urlparse.urlparse(string)
    if not all([url.scheme, url.netloc, url.path]):
        return celtypes.BoolType(False)
    return celtypes.BoolType(True)


def is_uri_ref(string: celtypes.Value) -> celpy.Result:
    if not isinstance(string, celtypes.StringType):
        msg = "invalid argument, expected string"
        raise celpy.CELEvalError(msg)
    url = urlparse.urlparse(string)
    if not all([url.scheme, url.path]) and url.fragment:
        return celtypes.BoolType(False)
    return celtypes.BoolType(True)


def is_hostname(string: celtypes.Value) -> celpy.Result:
    if not isinstance(string, celtypes.StringType):
        msg = "invalid argument, expected string"
        raise celpy.CELEvalError(msg)
    return celtypes.BoolType(_validate_hostname(string))


def is_uuid(string: celtypes.Value) -> celpy.Result:
    if not isinstance(string, celtypes.StringType):
        msg = "invalid argument, expected string"
        raise celpy.CELEvalError(msg)
    return celtypes.BoolType(validate_uuid(string))


def make_extra_funcs(locale: str) -> typing.Dict[str, celpy.CELFunction]:
    string_fmt = string_format.StringFormat(locale)
    return {
        # Missing standard functions
        "format": string_fmt.format,
        # protovalidate specific functions
        "isIpv4": is_ipv4,
        "isIpv6": is_ipv6,
        "isEmail": is_email,
        "isUri": is_uri,
        "isUriRef": is_uri_ref,
        "isHostname": is_hostname,
        "isUuid": is_uuid,
    }


EXTRA_FUNCS = make_extra_funcs("en_US")
