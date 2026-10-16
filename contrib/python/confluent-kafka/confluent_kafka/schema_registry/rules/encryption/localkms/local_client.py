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

import hashlib
from typing import Optional

from hkdf import hkdf_expand, hkdf_extract
from tink import KmsClient, aead
from tink.core import Registry
from tink.proto import aes_gcm_pb2, tink_pb2


class LocalKmsClient(KmsClient):
    def __init__(self, secret: Optional[str] = None):
        if secret is None:
            raise TypeError("secret cannot be None")
        self._aead = self._get_primitive(secret)

    def _get_primitive(self, secret: str) -> aead.Aead:
        key = self._get_key(secret)
        aes_gcm_key = aes_gcm_pb2.AesGcmKey(version=0, key_value=key)
        serialized_aes_gcm_key = aes_gcm_key.SerializeToString()
        key_template = aead.aead_key_templates.AES128_GCM_RAW
        key_data = tink_pb2.KeyData(
            type_url=key_template.type_url, value=serialized_aes_gcm_key, key_material_type=tink_pb2.KeyData.SYMMETRIC
        )
        return Registry().primitive(key_data, aead.Aead)

    def _get_key(self, secret: str) -> bytes:
        key = secret.encode("utf-8")
        prk = hkdf_extract(None, key, hash=hashlib.sha256)
        return hkdf_expand(prk, length=16, hash=hashlib.sha256)

    def does_support(self, key_uri: str) -> bool:
        return key_uri.startswith("local-kms://")

    def get_aead(self, key_uri: str) -> aead.Aead:
        return self._aead
