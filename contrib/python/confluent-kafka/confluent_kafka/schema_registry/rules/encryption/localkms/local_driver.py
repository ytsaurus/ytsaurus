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
from typing import Any, Dict, Optional

import tink
from tink import KmsClient

from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, register_kms_driver
from confluent_kafka.schema_registry.rules.encryption.localkms.local_client import LocalKmsClient

_PREFIX = "local-kms://"
_SECRET = "secret"


class LocalKmsDriver(KmsDriver):
    def __init__(self) -> None:
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        secret = conf.get(_SECRET)
        if secret is None:
            secret = os.getenv("LOCAL_SECRET")
        if secret is None:
            raise tink.TinkError("cannot load secret")
        return LocalKmsClient(secret)

    @classmethod
    def register(cls) -> None:
        register_kms_driver(LocalKmsDriver())
