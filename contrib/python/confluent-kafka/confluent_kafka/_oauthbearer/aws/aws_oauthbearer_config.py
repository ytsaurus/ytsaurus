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

"""Internal: validated ``sasl.oauthbearer.config`` dataclass + parser.

The full grammar (comma-separated ``key=value`` pairs, librdkafka grammar —
values may backslash-quote a comma, e.g. ``\\,``):

    region=<aws-region>            (required)
    audience=<oidc-audience>       (required)
    duration_seconds=<60..3600>    (default: 300)
    signing_algorithm=ES384|RS256  (default: ES384)
    sts_endpoint=<url>             (optional, FIPS / VPC)
    aws_debug=none|console         (default: none)
    tag_<name>=<value>             (zero or more JWT custom claims, max 50)

SASL extensions arrive separately via :data:`sasl_extensions` (parsed from
the typed ``sasl.oauthbearer.extensions`` config property). They are NOT
accepted inside this string under any ``extension_*`` prefix — that key
shape is rejected as an unknown key.
"""

from dataclasses import dataclass
from typing import Dict, Optional

from confluent_kafka._util.librdkafka_string_parser import parse_key_values

__all__ = [
    "CONFIG_KEY",
    "DEFAULT_SIGNING_ALGORITHM",
    "ALLOWED_SIGNING_ALGORITHMS",
    "MIN_DURATION_SECONDS",
    "MAX_DURATION_SECONDS",
    "DEFAULT_DURATION_SECONDS",
    "TAG_KEY_PREFIX",
    "MAX_TAGS",
    "AWS_DEBUG_NONE",
    "AWS_DEBUG_CONSOLE",
    "ALLOWED_AWS_DEBUG_VALUES",
    "AwsOAuthBearerConfig",
]


#: Config key carrying the AWS-path wire-grammar string.
CONFIG_KEY: str = "sasl.oauthbearer.config"

#: Default JWT signing algorithm.
DEFAULT_SIGNING_ALGORITHM: str = "ES384"

#: Signing algorithms accepted by AWS STS ``GetWebIdentityToken``.
ALLOWED_SIGNING_ALGORITHMS = ("ES384", "RS256")

#: Minimum / default / maximum token lifetime AWS STS allows
MIN_DURATION_SECONDS: int = 60
MAX_DURATION_SECONDS: int = 3600
DEFAULT_DURATION_SECONDS: int = 300

#: Wire-grammar prefix for STS ``Tags`` entries (e.g. ``tag_team=platform``).
TAG_KEY_PREFIX: str = "tag_"

#: AWS-enforced upper bound on number of tags per ``GetWebIdentityToken`` call.
MAX_TAGS: int = 50

#: Sentinel string values for ``aws_debug``.
AWS_DEBUG_NONE: str = "none"
AWS_DEBUG_CONSOLE: str = "console"

#: ``aws_debug`` values accepted by the Python client: ``none`` and ``console``.
ALLOWED_AWS_DEBUG_VALUES = (AWS_DEBUG_NONE, AWS_DEBUG_CONSOLE)


# Recognised non-tag keys for the wire grammar. Anything else (other than
# ``tag_<NAME>``) raises "Unknown key" during :meth:`AwsOAuthBearerConfig.parse`.
_RECOGNISED_KEYS = frozenset(
    {
        "region",
        "audience",
        "duration_seconds",
        "signing_algorithm",
        "sts_endpoint",
        "aws_debug",
    }
)

_NON_EMPTY_KEYS = frozenset(
    {
        "region",
        "audience",
        "signing_algorithm",
        "sts_endpoint",
        "aws_debug",
    }
)


@dataclass(frozen=True)
class AwsOAuthBearerConfig:
    """Immutable view of the AWS path's ``sasl.oauthbearer.config``."""

    region: str
    audience: str
    signing_algorithm: str = DEFAULT_SIGNING_ALGORITHM
    duration_seconds: int = DEFAULT_DURATION_SECONDS
    sts_endpoint: Optional[str] = None
    aws_debug: str = AWS_DEBUG_NONE
    tags: Optional[Dict[str, str]] = None
    sasl_extensions: Optional[Dict[str, str]] = None

    def __post_init__(self) -> None:
        """Final-state validation. Raises :class:`ValueError` on bad input."""
        if not isinstance(self.region, str) or self.region == "":
            raise ValueError(f"{CONFIG_KEY} 'region' must not be empty.")
        if not isinstance(self.audience, str) or self.audience == "":
            raise ValueError(f"{CONFIG_KEY} 'audience' must not be empty.")
        if self.signing_algorithm not in ALLOWED_SIGNING_ALGORITHMS:
            raise ValueError(
                f"{CONFIG_KEY} 'signing_algorithm' must be 'ES384' or 'RS256'; " f"got {self.signing_algorithm!r}."
            )
        # bool is-a int in Python — reject explicitly so True/False doesn't
        # slip through as 1/0.
        if not isinstance(self.duration_seconds, int) or isinstance(self.duration_seconds, bool):
            raise ValueError(f"{CONFIG_KEY} 'duration_seconds' must be an integer.")
        if not (MIN_DURATION_SECONDS <= self.duration_seconds <= MAX_DURATION_SECONDS):
            raise ValueError(
                f"{CONFIG_KEY} 'duration_seconds' must be between "
                f"{MIN_DURATION_SECONDS} and {MAX_DURATION_SECONDS} inclusive; "
                f"got {self.duration_seconds}."
            )
        if self.sts_endpoint is not None and self.sts_endpoint == "":
            raise ValueError(f"{CONFIG_KEY} 'sts_endpoint' must not be empty.")
        if self.aws_debug not in ALLOWED_AWS_DEBUG_VALUES:
            raise ValueError(f"{CONFIG_KEY} 'aws_debug' must be one of: none, console. Got {self.aws_debug!r}.")
        if self.tags is not None:
            if not isinstance(self.tags, dict):
                raise ValueError(f"{CONFIG_KEY} 'tags' must be a dict.")
            if len(self.tags) > MAX_TAGS:
                raise ValueError(f"{CONFIG_KEY} has {len(self.tags)} tags; AWS allows at " f"most {MAX_TAGS}.")
        if self.sasl_extensions is not None and not isinstance(self.sasl_extensions, dict):
            raise ValueError("sasl_extensions, if set, must be a dict.")

    @classmethod
    def parse(
        cls,
        raw: str,
        sasl_extensions: Optional[Dict[str, str]] = None,
    ) -> "AwsOAuthBearerConfig":
        """Parse the verbatim ``sasl.oauthbearer.config`` value.

        Comma-separated ``key=value`` tokens; the union of recognised
        keys plus ``tag_<NAME>`` entries. Anything else raises
        :class:`ValueError`. Empty values for required-non-empty keys raise
        the same. Duplicate keys → last-wins.

        :param raw: The verbatim ``sasl.oauthbearer.config`` string.
        :param sasl_extensions: Pre-parsed dict from the sibling
            ``sasl.oauthbearer.extensions`` property (see
            :mod:`.sasl_extensions_parser`). Stored on the config
            unchanged.
        :raises TypeError: ``raw`` is ``None``.
        :raises ValueError: grammar, range, or enum violations.
        """
        if raw is None:
            raise TypeError("raw must not be None")

        region: Optional[str] = None
        audience: Optional[str] = None
        signing_algorithm: str = DEFAULT_SIGNING_ALGORITHM
        duration_seconds: int = DEFAULT_DURATION_SECONDS
        sts_endpoint: Optional[str] = None
        aws_debug: str = AWS_DEBUG_NONE
        tags: Optional[Dict[str, str]] = None

        for key, value in parse_key_values(raw, ",", CONFIG_KEY):
            if key in _NON_EMPTY_KEYS and value == "":
                raise ValueError(f"{CONFIG_KEY} {key!r} must not be empty.")

            if key == "region":
                region = value
            elif key == "audience":
                audience = value
            elif key == "signing_algorithm":
                signing_algorithm = value
            elif key == "duration_seconds":
                try:
                    duration_seconds = int(value)
                except ValueError as exc:
                    raise ValueError(f"{CONFIG_KEY} 'duration_seconds' must be an integer; " f"got {value!r}.") from exc
            elif key == "sts_endpoint":
                sts_endpoint = value
            elif key == "aws_debug":
                # Normalize case so downstream comparisons against
                # ALLOWED_AWS_DEBUG_VALUES are straightforward.
                aws_debug = value.lower()
            elif key.startswith(TAG_KEY_PREFIX):
                tag_name = key[len(TAG_KEY_PREFIX) :]
                if tag_name == "":
                    raise ValueError(f"{CONFIG_KEY} tag key {key!r} has empty name.")
                if tags is None:
                    tags = {}
                tags[tag_name] = value  # last-wins on duplicate tag names
            else:
                raise ValueError(f"Unknown key {key!r} in {CONFIG_KEY}.")

        if region is None:
            raise ValueError(f"'region' is required in {CONFIG_KEY}.")
        if audience is None:
            raise ValueError(f"'audience' is required in {CONFIG_KEY}.")

        return cls(
            region=region,
            audience=audience,
            signing_algorithm=signing_algorithm,
            duration_seconds=duration_seconds,
            sts_endpoint=sts_endpoint,
            aws_debug=aws_debug,
            tags=tags,
            sasl_extensions=sasl_extensions,
        )
