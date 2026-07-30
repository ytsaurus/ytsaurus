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

"""Internal entry-point for AWS IAM OAUTHBEARER autowire.

This module is internal to ``confluent-kafka`` and not part of the public
API — applications never import it or call :func:`create_handler` directly.
The C dispatcher in ``src/confluent_kafka/src/confluent_kafka.c`` reaches it
via::

    PyImport_ImportModule("confluent_kafka._oauthbearer.aws.aws_autowire")

and resolves :func:`create_handler` by name. The marker-key check is
performed in core; :func:`create_handler` is invoked only when the C
dispatcher has decided to autowire the AWS path.

Users activate this path through configuration only — four config keys::

    "sasl.oauthbearer.method":                        "oidc"
    "sasl.oauthbearer.metadata.authentication.type":  "aws_iam"
    "sasl.oauthbearer.config":                        "region=...,audience=..."
    "sasl.oauthbearer.extensions":                    "key=val,..."   # optional

:func:`create_handler` has a frozen internal contract with the C dispatcher:

* arity:   2 positional parameters
* names:   ``sasl_oauthbearer_config``, ``sasl_oauthbearer_extensions``
* types:   ``str``, ``Optional[str]``
* return:  :data:`OAuthBearerCallback`

The C dispatcher and this module ship together and must stay in sync; changing
the contract would break autowiring. Guarded by
``tests/oauthbearer/aws/test_contract.py``.
"""

from typing import Callable, Dict, Optional, Tuple

from . import sasl_extensions_parser
from .aws_iam_marker import AWS_IAM_MARKER_KEY, AWS_IAM_MARKER_VALUE
from .aws_oauthbearer_config import CONFIG_KEY, AwsOAuthBearerConfig
from .aws_sts_token_provider import AwsStsTokenProvider

__all__ = ["create_handler", "OAuthBearerCallback"]

OAuthBearerCallback = Callable[[str], Tuple[str, float, str, Dict[str, str]]]


def create_handler(
    sasl_oauthbearer_config: str,
    sasl_oauthbearer_extensions: Optional[str],
) -> OAuthBearerCallback:
    """Build an OAUTHBEARER refresh callback from the two OAUTHBEARER config strings.

    :param sasl_oauthbearer_config: The verbatim ``sasl.oauthbearer.config``
        value (whitespace-separated ``key=value`` pairs). Must be non-empty.
    :param sasl_oauthbearer_extensions: The verbatim
        ``sasl.oauthbearer.extensions`` value (comma-separated ``key=value``
        pairs, RFC 7628 §3.1). May be ``None`` or empty when the user has
        no extensions configured.

    :returns: A callable matching :data:`OAuthBearerCallback`.

    :raises ValueError: ``sasl_oauthbearer_config`` is ``None`` or empty;
        the wire-grammar parse fails (unknown key, malformed token, missing
        required field, range/enum violation, etc.).
    :raises ImportError: the installed boto3 predates the minimum required by
        the AWS IAM path (boto3 is present but too old for STS
        ``GetWebIdentityToken``).
    :raises RuntimeError: AWS SDK reachability or initialisation failure
        (e.g. unknown region, malformed ``sts_endpoint``).
    """
    if not sasl_oauthbearer_config:
        raise ValueError(
            f"'{AWS_IAM_MARKER_KEY}={AWS_IAM_MARKER_VALUE}' is set but "
            f"'{CONFIG_KEY}' is missing or empty. The AWS IAM autowire path "
            f"requires region and audience to be supplied via "
            f"{CONFIG_KEY} (e.g. \"region=us-east-1,audience=https://...\")."
        )

    sasl_extensions = sasl_extensions_parser.parse(sasl_oauthbearer_extensions)
    config = AwsOAuthBearerConfig.parse(sasl_oauthbearer_config, sasl_extensions)
    provider = AwsStsTokenProvider(config)
    return provider.token
