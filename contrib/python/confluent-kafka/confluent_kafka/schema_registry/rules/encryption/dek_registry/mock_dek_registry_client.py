#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
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
#

import time
from typing import Dict, Optional

from confluent_kafka.schema_registry import SchemaRegistryError
from confluent_kafka.schema_registry.rules.encryption.dek_registry.dek_registry_client import (
    Dek,
    DekAlgorithm,
    DekId,
    DekRegistryClient,
    Kek,
    KekId,
    KekKmsProps,
)


class MockDekRegistryClient(DekRegistryClient):
    """
    A Mock DEK Registry client.

    Args:
        conf (dict): DEK Registry client configuration.

    """  # noqa: E501

    def __init__(self, conf: dict):
        super().__init__(conf)

    def register_kek(
        self,
        name: str,
        kms_type: str,
        kms_key_id: str,
        shared: bool = False,
        kms_props: Optional[Dict[str, str]] = None,
        doc: Optional[str] = None,
    ) -> Kek:
        cache_key = KekId(name=name, deleted=False)
        kek = self._kek_cache.get_kek(cache_key)
        if kek is not None:
            return kek

        kek = Kek(
            name=name,
            kms_type=kms_type,
            kms_key_id=kms_key_id,
            kms_props=KekKmsProps.from_dict(kms_props) if kms_props is not None else None,
            doc=doc,
            shared=shared,
        )

        self._kek_cache.set(cache_key, kek)

        return kek

    def get_kek(self, name: str, deleted: bool = False) -> Kek:
        cache_key = KekId(name=name, deleted=deleted)
        kek = self._kek_cache.get_kek(cache_key)
        if kek is not None:
            return kek

        raise SchemaRegistryError(404, 40470, "Key Not Found")

    def register_dek(
        self,
        kek_name: str,
        subject: str,
        encrypted_key_material: str,
        algorithm: DekAlgorithm = DekAlgorithm.AES256_GCM,
        version: int = 1,
    ) -> Dek:
        cache_key = DekId(kek_name=kek_name, subject=subject, version=version, algorithm=algorithm, deleted=False)
        dek = self._dek_cache.get_dek(cache_key)
        if dek is not None:
            return dek

        dek = Dek(
            kek_name=kek_name,
            subject=subject,
            version=version,
            algorithm=algorithm,
            encrypted_key_material=encrypted_key_material,
            ts=int(round(time.time() * 1000)),
        )

        self._dek_cache.set(cache_key, dek)

        return dek

    def get_dek(
        self,
        kek_name: str,
        subject: str,
        algorithm: DekAlgorithm = DekAlgorithm.AES256_GCM,
        version: int = 1,
        deleted: bool = False,
    ) -> Dek:
        if version == -1:
            # Find the latest version
            latest_version = 0
            for dek_id in self._dek_cache.get_dek_ids():
                if dek_id.kek_name == kek_name and dek_id.subject == subject and dek_id.algorithm == algorithm:
                    latest_version = max(latest_version, dek_id.version)
            if latest_version == 0:
                raise SchemaRegistryError(404, 40470, "Key Not Found")
            version = latest_version

        cache_key = DekId(kek_name=kek_name, subject=subject, version=version, algorithm=algorithm, deleted=False)
        dek = self._dek_cache.get_dek(cache_key)
        if dek is not None:
            return dek

        raise SchemaRegistryError(404, 40470, "Key Not Found")
