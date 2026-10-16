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
import os
from typing import Any, Dict, Optional, Tuple, Union

from tink import KmsClient

from confluent_kafka.schema_registry.rules.encryption.hcvault.hcvault_client import HcVaultKmsClient
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, register_kms_driver

_PREFIX = "hcvault://"
_TOKEN_ID = "token.id"
_NAMESPACE = "namespace"
_APPROLE_ROLE_ID = "approle.role.id"
_APPROLE_SECRET_ID = "approle.secret.id"
_SSL_CA_LOCATION = "ssl.ca.location"
_SSL_CERTIFICATE_LOCATION = "ssl.certificate.location"
_SSL_KEY_LOCATION = "ssl.key.location"


class HcVaultKmsDriver(KmsDriver):
    def __init__(self) -> None:
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url
        token = conf.get(_TOKEN_ID)
        if token is None:
            token = os.getenv("VAULT_TOKEN")
        namespace = conf.get(_NAMESPACE)
        if namespace is None:
            namespace = os.getenv("VAULT_NAMESPACE")
        role_id = conf.get(_APPROLE_ROLE_ID)
        if role_id is None:
            role_id = os.getenv("VAULT_APPROLE_ROLE_ID")
        secret_id = conf.get(_APPROLE_SECRET_ID)
        if secret_id is None:
            secret_id = os.getenv("VAULT_APPROLE_SECRET_ID")
        verify = self._get_verify(conf)
        cert = self._get_cert(conf)
        return HcVaultKmsClient(uri_prefix, token, namespace, role_id, secret_id, verify, cert)

    @staticmethod
    def _get_verify(conf: Dict[str, Any]) -> Union[bool, str]:
        # A CA bundle path enables verification against that bundle. The standard
        # VAULT_CACERT environment variable is honored as a fallback. An empty or
        # unset value must NOT disable verification: requests/hvac treat a falsy
        # ``verify`` as "do not verify the server certificate", so an empty string
        # (e.g. ``VAULT_CACERT=""`` or ``ssl.ca.location=""``) would silently
        # reopen the certificate-validation hole. Treat any falsy value as
        # unconfigured and fall back to the secure default of True.
        ca_location = conf.get(_SSL_CA_LOCATION) or os.getenv("VAULT_CACERT")
        if ca_location:
            return ca_location
        # Verification is always enabled by default.
        return True

    @staticmethod
    def _get_cert(conf: Dict[str, Any]) -> Optional[Union[str, Tuple[str, str]]]:
        cert_location = conf.get(_SSL_CERTIFICATE_LOCATION)
        if cert_location is None:
            cert_location = os.getenv("VAULT_CLIENT_CERT")
        key_location = conf.get(_SSL_KEY_LOCATION)
        if key_location is None:
            key_location = os.getenv("VAULT_CLIENT_KEY")
        if key_location is not None and cert_location is None:
            raise ValueError(f"{_SSL_CERTIFICATE_LOCATION} required when configuring {_SSL_KEY_LOCATION}")
        if cert_location is not None and key_location is not None:
            return cert_location, key_location
        if cert_location is not None:
            return cert_location
        return None

    @classmethod
    def register(cls) -> None:
        register_kms_driver(HcVaultKmsDriver())
