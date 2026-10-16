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

from azure.core.credentials import TokenCredential
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from tink import KmsClient

from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_client import AzureKmsClient
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, register_kms_driver

_PREFIX = "azure-kms://"
_TENANT_ID = 'tenant.id'
_CLIENT_ID = 'client.id'
_CLIENT_SECRET = 'client.secret'


class AzureKmsDriver(KmsDriver):
    def __init__(self) -> None:
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url
        tenant_id = conf.get(_TENANT_ID)
        client_id = conf.get(_CLIENT_ID)
        client_secret = conf.get(_CLIENT_SECRET)

        creds: TokenCredential
        if tenant_id is None or client_id is None or client_secret is None:
            creds = DefaultAzureCredential()
        else:
            creds = ClientSecretCredential(tenant_id, client_id, client_secret)

        return AzureKmsClient(uri_prefix, creds)

    @classmethod
    def register(cls) -> None:
        register_kms_driver(AzureKmsDriver())
