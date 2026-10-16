# Copyright 2024 Confluent Inc.
# Copyright 2019 Google LLC
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

from typing import Optional

import tink
from google.cloud import kms_v1
from google.oauth2 import service_account
from tink import aead
from tink.integration.gcpkms import GcpKmsClient
from tink.integration.gcpkms._gcp_kms_client import _GcpKmsAead

GCP_KEYURI_PREFIX = 'gcp-kms://'


class _GcpKmsClient(GcpKmsClient):
    """Basic GCP client for AEAD."""

    def __init__(self, key_uri: Optional[str], credentials: Optional[service_account.Credentials]) -> None:
        """Creates a new GcpKmsClient that is bound to the key specified in 'key_uri'.

        Uses the specified credentials when communicating with the KMS.

        Args:
          key_uri: The URI of the key the client should be bound to. If it is None
              or empty, then the client is not bound to any particular key.
          credentials: The service account credentials.

        Raises:
          TinkError: If the key uri is not valid.
        """

        super().__init__(key_uri, None)
        if not key_uri:
            self._key_uri = None
        elif key_uri.startswith(GCP_KEYURI_PREFIX):
            self._key_uri = key_uri
        else:
            raise tink.TinkError('Invalid key_uri.')
        self._client = kms_v1.KeyManagementServiceClient(credentials=credentials)

    def does_support(self, key_uri: str) -> bool:
        """Returns true iff this client supports KMS key specified in 'key_uri'.

        Args:
          key_uri: URI of the key to be checked.

        Returns:
          A boolean value which is true if the key is supported and false otherwise.
        """
        if not self._key_uri:
            return key_uri.startswith(GCP_KEYURI_PREFIX)
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
        if not key_uri.startswith(GCP_KEYURI_PREFIX):
            raise tink.TinkError('Invalid key_uri.')
        key_id = key_uri[len(GCP_KEYURI_PREFIX) :]
        return _GcpKmsAead(self._client, key_id)
