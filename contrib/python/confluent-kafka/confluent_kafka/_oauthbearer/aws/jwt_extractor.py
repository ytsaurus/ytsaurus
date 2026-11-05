# Copyright 2026 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Internal: extracts the ``sub`` claim from an unverified JWT.

No signature verification — STS signs, broker validates.
"""

import base64
import binascii
import json

__all__ = ["extract_sub"]


_MAX_TOKEN_LENGTH_CHARS: int = 8192


def extract_sub(jwt: str) -> str:
    """Return the ``sub`` claim from the JWT payload.

    :raises ValueError: ``jwt`` is null, empty, oversized, has the wrong
        segment count, fails base64url decoding, isn't valid JSON, isn't a
        JSON object, or has no ``sub`` string claim (or its value is empty).
    """
    if jwt is None:
        raise ValueError("JWT is null.")
    if jwt == "":
        raise ValueError("JWT is empty.")
    if len(jwt) > _MAX_TOKEN_LENGTH_CHARS:
        raise ValueError(f"JWT length {len(jwt)} exceeds maximum allowed " f"({_MAX_TOKEN_LENGTH_CHARS}).")

    parts = jwt.split(".")
    if len(parts) != 3:
        raise ValueError(f"JWT must have exactly 3 '.'-separated segments; got {len(parts)}.")

    payload_bytes = _decode_base64url_segment(parts[1])
    try:
        payload_string = payload_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"JWT payload is not valid UTF-8: {exc}") from exc

    try:
        token = json.loads(payload_string)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JWT payload is not valid JSON: {exc}") from exc

    if not isinstance(token, dict):
        raise ValueError("JWT payload is not a JSON object.")

    if "sub" not in token:
        raise ValueError("JWT payload is missing a 'sub' string claim.")
    sub = token["sub"]
    if not isinstance(sub, str):
        raise ValueError("JWT payload is missing a 'sub' string claim.")
    if sub == "":
        raise ValueError("JWT 'sub' claim value is empty.")
    return sub


def _decode_base64url_segment(segment: str) -> bytes:
    if len(segment) == 0:
        raise ValueError("JWT payload segment is empty.")

    s = segment.replace("-", "+").replace("_", "/")
    remainder = len(s) % 4
    if remainder == 0:
        pass
    elif remainder == 2:
        s += "=="
    elif remainder == 3:
        s += "="
    else:
        raise ValueError("JWT payload segment has invalid base64url length.")

    try:
        return base64.b64decode(s.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError, ValueError) as exc:
        raise ValueError(f"JWT payload segment is not valid base64url: {exc}") from exc
