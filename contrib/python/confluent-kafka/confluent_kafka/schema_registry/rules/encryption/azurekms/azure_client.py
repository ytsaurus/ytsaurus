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
"""A client for Google Cloud KMS."""

import tink
from azure.core.credentials import TokenCredential
from azure.keyvault.keys.crypto import CryptographyClient, EncryptionAlgorithm
from tink import aead

from confluent_kafka.schema_registry.rules.encryption.azurekms.azure_aead import AzureKmsAead

AZURE_KEYURI_PREFIX = 'azure-kms://'


class AzureKmsClient(tink.KmsClient):
    """Basic Azure client for AEAD."""

    def __init__(self, key_uri: str, credentials: TokenCredential) -> None:
        """Creates a new AzureKmsClient that is bound to the key specified in 'key_uri'.

        Uses the specified credentials when communicating with the KMS.

        Args:
          key_uri: The URI of the key the client should be bound to.
          credentials: The token credentials.

        Raises:
          TinkError: If the key uri is not valid.
        """

        if key_uri.startswith(AZURE_KEYURI_PREFIX):
            self._key_uri = key_uri
        else:
            raise tink.TinkError('Invalid key_uri.')

        key_id = key_uri[len(AZURE_KEYURI_PREFIX) :]
        self._client = CryptographyClient(key_id, credentials)

    def does_support(self, key_uri: str) -> bool:
        """Returns true iff this client supports KMS key specified in 'key_uri'.

        Args:
          key_uri: URI of the key to be checked.

        Returns:
          A boolean value which is true if the key is supported and false otherwise.
        """
        if not self._key_uri:
            return key_uri.startswith(AZURE_KEYURI_PREFIX)
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
        if not key_uri.startswith(AZURE_KEYURI_PREFIX):
            raise tink.TinkError('Invalid key_uri.')
        return AzureKmsAead(self._client, EncryptionAlgorithm.rsa_oaep_256)
