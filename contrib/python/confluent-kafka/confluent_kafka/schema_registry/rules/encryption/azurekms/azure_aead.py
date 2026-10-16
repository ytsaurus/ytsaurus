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
from azure.keyvault.keys.crypto import CryptographyClient, EncryptionAlgorithm
from tink import aead


class AzureKmsAead(aead.Aead):
    """Implements the Aead interface for Azure KMS."""

    def __init__(self, client: CryptographyClient, algorithm: EncryptionAlgorithm) -> None:
        if not client:
            raise tink.TinkError('client cannot be null.')
        self.client = client
        self.algorithm = algorithm

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        try:
            response = self.client.encrypt(self.algorithm, plaintext)
            return response.ciphertext
        except ValueError as e:
            raise tink.TinkError(e)

    def decrypt(self, ciphertext: bytes, associated_data: bytes) -> bytes:
        try:
            response = self.client.decrypt(self.algorithm, ciphertext)
            return response.plaintext
        except ValueError as e:
            raise tink.TinkError(e)
