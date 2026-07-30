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

"""Internal: parser for the ``sasl.oauthbearer.extensions`` config property.

The ``sasl.oauthbearer.extensions`` config carries RFC 7628 SASL
extensions as a comma-separated ``key=value`` list.
"""

from typing import Dict, Optional

from confluent_kafka._util.librdkafka_string_parser import parse_key_values

__all__ = ["CONFIG_KEY", "parse"]


#: Config key carrying the SASL extensions list.
CONFIG_KEY: str = "sasl.oauthbearer.extensions"


def parse(raw: Optional[str]) -> Optional[Dict[str, str]]:
    """Parse the verbatim ``sasl.oauthbearer.extensions`` value into a dict.

    The grammar mirrors the cross-language convention for consistency.

    Returns ``None`` for ``None`` / empty input so the autowire layer can
    short-circuit without constructing an empty dict.

    :raises ValueError: A token is missing ``=`` or has an empty key.
    """
    if raw is None or raw == "":
        return None

    result: Dict[str, str] = {}
    for key, value in parse_key_values(raw, ",", CONFIG_KEY):
        # Last-wins on duplicate keys, mirroring librdkafka.
        result[key] = value

    return result if result else None
