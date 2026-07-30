# Copyright 2024 Confluent Inc.
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
from typing import Any, Dict, Optional

import tink
from google.oauth2 import service_account
from tink import KmsClient

from confluent_kafka.schema_registry.rules.encryption.gcpkms.gcp_client import _GcpKmsClient
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, register_kms_driver

_PREFIX = "gcp-kms://"
_ACCOUNT_TYPE = "account.type"
_CLIENT_ID = "client.id"
_CLIENT_EMAIL = "client.email"
_PRIVATE_KEY_ID = "private.key.id"
_PRIVATE_KEY = "private.key"
_TOKEN_URI = "token.uri"


class GcpKmsDriver(KmsDriver):
    def __init__(self) -> None:
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url
        account_type = conf.get(_ACCOUNT_TYPE)
        if account_type is None:
            account_type = "service_account"
        if account_type != "service_account":
            raise tink.TinkError("account.type must be 'service_account'")
        client_id = conf.get(_CLIENT_ID)
        client_email = conf.get(_CLIENT_EMAIL)
        private_key_id = conf.get(_PRIVATE_KEY_ID)
        private_key = conf.get(_PRIVATE_KEY)
        token_uri = conf.get(_TOKEN_URI)
        if token_uri is None:
            token_uri = "https://oauth2.googleapis.com/token"

        if client_id is None or client_email is None or private_key_id is None or private_key is None:
            creds = None
        else:
            creds = service_account.Credentials.from_service_account_info(
                {
                    "type": account_type,
                    "client_id": client_id,
                    "client_email": client_email,
                    "private_key_id": private_key_id,
                    "private_key": private_key,
                    "token_uri": token_uri,
                }
            )

        return _GcpKmsClient(uri_prefix, creds)

    @classmethod
    def register(cls) -> None:
        register_kms_driver(GcpKmsDriver())


def _key_uri_to_key_arn(key_uri: str) -> str:
    if not key_uri.startswith(_PREFIX):
        raise tink.TinkError('invalid key URI')
    return key_uri[len(_PREFIX) :]


def _get_region_from_key_arn(key_arn: str) -> str:
    # An AWS key ARN is of the form
    # arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab.
    key_arn_parts = key_arn.split(':')
    if len(key_arn_parts) < 6:
        raise tink.TinkError('invalid key id')
    return key_arn_parts[3]
