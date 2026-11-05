# Copyright 2024 Confluent Inc.
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
"""A client for Hashicorp Vault."""

from typing import Optional, Tuple, Union
from urllib.parse import urlparse

import hvac
import tink
from tink import aead
from tink.integration.hcvault import new_aead

VAULT_KEYURI_PREFIX = 'hcvault://'


class HcVaultKmsClient(tink.KmsClient):
    """Basic HashiCorp Vault client for AEAD."""

    def __init__(
        self,
        key_uri: str,
        token: Optional[str],
        ns: Optional[str] = None,
        role_id: Optional[str] = None,
        secret_id: Optional[str] = None,
        verify: Union[bool, str] = True,
        cert: Optional[Union[str, Tuple[str, str]]] = None,
    ) -> None:
        """Creates a new HcVaultKmsClient that is bound to the key specified in 'key_uri'.

        Uses the specified credentials when communicating with the KMS.

        Args:
          key_uri: The URI of the key the client should be bound to.
          token: The Vault token.
          ns: The Vault namespace.
          role_id: The AppRole role id.
          secret_id: The AppRole secret id.
          verify: Whether to verify the Vault server's TLS certificate. Either a
            boolean, or the path to a CA bundle to use for verification. Defaults
            to True; setting it to False disables certificate verification and is
            insecure.
          cert: Client certificate for mutual TLS. Either the path to a single PEM
            file containing the certificate and key, or a (cert, key) tuple of paths.

        Raises:
          TinkError: If the key uri is not valid.
        """

        if key_uri.startswith(VAULT_KEYURI_PREFIX):
            self._key_uri = key_uri
        else:
            raise tink.TinkError('Invalid key_uri.')

        parsed = urlparse(key_uri[len(VAULT_KEYURI_PREFIX) :])
        vault_url = parsed.scheme + '://' + parsed.netloc
        self._client = hvac.Client(url=vault_url, token=token, namespace=ns, verify=verify, cert=cert)
        if role_id and secret_id and self._client is not None:
            self._client.auth.approle.login(role_id=role_id, secret_id=secret_id)

    def does_support(self, key_uri: str) -> bool:
        """Returns true iff this client supports KMS key specified in 'key_uri'.

        Args:
          key_uri: URI of the key to be checked.

        Returns:
          A boolean value which is true if the key is supported and false otherwise.
        """
        if not self._key_uri:
            return key_uri.startswith(VAULT_KEYURI_PREFIX)
        return key_uri == self._key_uri

    def get_aead(self, key_uri: str) -> aead.Aead:
        """Returns an Aead-primitive backed by KMS key specified by 'key_uri'.

        Args:
          key_uri: URI of the key which should be used.

        Returns:
          An Aead object.
        """
        if self._key_uri and self._key_uri != key_uri:
            raise tink.TinkError('This client is bound to %s and cannot use key %s' % (self._key_uri, key_uri))
        if not key_uri.startswith(VAULT_KEYURI_PREFIX):
            raise tink.TinkError('Invalid key_uri.')
        key_id = key_uri[len(VAULT_KEYURI_PREFIX) :]
        parsed = urlparse(key_id)
        return new_aead(parsed.path, self._client)
