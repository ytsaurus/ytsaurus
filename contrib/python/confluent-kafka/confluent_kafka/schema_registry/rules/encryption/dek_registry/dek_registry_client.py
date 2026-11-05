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

import base64
import threading
import urllib.parse
from enum import Enum
from threading import Lock
from typing import Any, Dict, List, Optional, Type, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from confluent_kafka.schema_registry.schema_registry_client import _RestClient

T = TypeVar("T")


@_attrs_define
class KekKmsProps:
    properties: Dict[str, str] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        field_dict: Dict[str, Any] = {}
        field_dict.update(self.properties)

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        kek_kms_props = cls()

        kek_kms_props.properties = d  # type: ignore[attr-defined]
        return kek_kms_props


@_attrs_define
class Kek:
    name: Optional[str]
    kms_type: Optional[str]
    kms_key_id: Optional[str]
    kms_props: Optional[KekKmsProps]
    doc: Optional[str]
    shared: Optional[bool]
    ts: Optional[int] = _attrs_field(default=None)
    deleted: Optional[bool] = _attrs_field(default=None)

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        kms_type = self.kms_type

        kms_key_id = self.kms_key_id

        _kms_props: Optional[Dict[str, Any]] = None
        if self.kms_props is not None:
            _kms_props = self.kms_props.to_dict()

        doc = self.doc

        shared = self.shared

        ts = self.ts

        deleted = self.deleted

        field_dict: Dict[str, Any] = {}
        if name is not None:
            field_dict["name"] = name
        if kms_type is not None:
            field_dict["kmsType"] = kms_type
        if kms_key_id is not None:
            field_dict["kmsKeyId"] = kms_key_id
        if _kms_props is not None:
            field_dict["kmsProps"] = _kms_props
        if doc is not None:
            field_dict["doc"] = doc
        if shared is not None:
            field_dict["shared"] = shared
        if ts is not None:
            field_dict["ts"] = ts
        if deleted is not None:
            field_dict["deleted"] = deleted

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name", None)

        kms_type = d.pop("kmsType", None)

        kms_key_id = d.pop("kmsKeyId", None)

        _kms_props: Optional[Dict[str, Any]] = d.pop("kmsProps", None)
        kms_props: Optional[KekKmsProps]
        if _kms_props is None:
            kms_props = None
        else:
            kms_props = KekKmsProps.from_dict(_kms_props)

        doc = d.pop("doc", None)

        shared = d.pop("shared", None)

        ts = d.pop("ts", None)

        deleted = d.pop("deleted", None)

        kek = cls(  # type: ignore[call-arg]
            name=name,
            kms_type=kms_type,
            kms_key_id=kms_key_id,
            kms_props=kms_props,
            doc=doc,
            shared=shared,
            ts=ts,
            deleted=deleted,
        )

        return kek


@_attrs_define
class CreateKekRequest:
    name: Optional[str]
    kms_type: Optional[str]
    kms_key_id: Optional[str]
    kms_props: Optional[KekKmsProps]
    doc: Optional[str]
    shared: Optional[bool]

    def to_dict(self) -> Dict[str, Any]:
        name = self.name

        kms_type = self.kms_type

        kms_key_id = self.kms_key_id

        _kms_props: Optional[Dict[str, Any]] = None
        if self.kms_props is not None:
            _kms_props = self.kms_props.to_dict()

        doc = self.doc

        shared = self.shared

        field_dict: Dict[str, Any] = {}
        if name is not None:
            field_dict["name"] = name
        if kms_type is not None:
            field_dict["kmsType"] = kms_type
        if kms_key_id is not None:
            field_dict["kmsKeyId"] = kms_key_id
        if _kms_props is not None:
            field_dict["kmsProps"] = _kms_props
        if doc is not None:
            field_dict["doc"] = doc
        if shared is not None:
            field_dict["shared"] = shared

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        name = d.pop("name", None)

        kms_type = d.pop("kmsType", None)

        kms_key_id = d.pop("kmsKeyId", None)

        _kms_props: Optional[Dict[str, Any]] = d.pop("kmsProps", None)
        kms_props: Optional[KekKmsProps]
        if _kms_props is None:
            kms_props = None
        else:
            kms_props = KekKmsProps.from_dict(_kms_props)

        doc = d.pop("doc", None)

        shared = d.pop("shared", None)

        create_kek_request = cls(  # type: ignore[call-arg]
            name=name,
            kms_type=kms_type,
            kms_key_id=kms_key_id,
            kms_props=kms_props,
            doc=doc,
            shared=shared,
        )

        return create_kek_request


class DekAlgorithm(str, Enum):
    AES128_GCM = "AES128_GCM"
    AES256_GCM = "AES256_GCM"
    AES256_SIV = "AES256_SIV"

    def __str__(self) -> str:
        return str(self.value)


@_attrs_define
class Dek:
    kek_name: Optional[str]
    subject: Optional[str]
    version: Optional[int]
    algorithm: Optional[DekAlgorithm]
    encrypted_key_material: Optional[str]
    encrypted_key_material_bytes: Optional[bytes] = _attrs_field(init=False, eq=False, order=False, default=None)
    key_material: Optional[str] = _attrs_field(default=None)
    key_material_bytes: Optional[bytes] = _attrs_field(init=False, eq=False, order=False, default=None)
    ts: Optional[int] = _attrs_field(default=None)
    deleted: Optional[bool] = _attrs_field(default=None)
    _lock: threading.Lock = _attrs_field(factory=threading.Lock, init=False, eq=False, order=False)

    def get_encrypted_key_material_bytes(self) -> Optional[bytes]:
        if self.encrypted_key_material is None:
            return None
        if self.encrypted_key_material_bytes is None:
            with self._lock:
                if self.encrypted_key_material_bytes is None:
                    self.encrypted_key_material_bytes = base64.b64decode(self.encrypted_key_material)
        return self.encrypted_key_material_bytes

    def get_key_material_bytes(self) -> Optional[bytes]:
        if self.key_material is None:
            return None
        if self.key_material_bytes is None:
            with self._lock:
                if self.key_material_bytes is None:
                    self.key_material_bytes = base64.b64decode(self.key_material)
        return self.key_material_bytes

    def set_key_material(self, key_material_bytes: bytes):
        with self._lock:
            if key_material_bytes is None:
                self.key_material = None
            else:
                self.key_material = base64.b64encode(key_material_bytes).decode("utf-8")

    def to_dict(self) -> Dict[str, Any]:
        kek_name = self.kek_name

        subject = self.subject

        version = self.version

        algorithm: Optional[str] = None
        if self.algorithm is not None:
            algorithm = self.algorithm

        encrypted_key_material = self.encrypted_key_material

        key_material = self.key_material

        ts = self.ts

        deleted = self.deleted

        field_dict: Dict[str, Any] = {}
        if kek_name is not None:
            field_dict["kekName"] = kek_name
        if subject is not None:
            field_dict["subject"] = subject
        if version is not None:
            field_dict["version"] = version
        if algorithm is not None:
            field_dict["algorithm"] = algorithm
        if encrypted_key_material is not None:
            field_dict["encryptedKeyMaterial"] = encrypted_key_material
        if key_material is not None:
            field_dict["keyMaterial"] = key_material
        if ts is not None:
            field_dict["ts"] = ts
        if deleted is not None:
            field_dict["deleted"] = deleted

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        kek_name = d.pop("kekName", None)

        subject = d.pop("subject", None)

        version = d.pop("version", None)

        _algorithm = d.pop("algorithm", None)
        algorithm: Optional[DekAlgorithm]
        if _algorithm is None:
            algorithm = None
        else:
            algorithm = DekAlgorithm(_algorithm)

        encrypted_key_material = d.pop("encryptedKeyMaterial", None)

        key_material = d.pop("keyMaterial", None)

        ts = d.pop("ts", None)

        deleted = d.pop("deleted", None)

        dek = cls(  # type: ignore[call-arg]
            kek_name=kek_name,
            subject=subject,
            version=version,
            algorithm=algorithm,
            encrypted_key_material=encrypted_key_material,
            key_material=key_material,
            ts=ts,
            deleted=deleted,
        )

        return dek


@_attrs_define
class CreateDekRequest:
    subject: Optional[str]
    version: Optional[int]
    algorithm: Optional[DekAlgorithm]
    encrypted_key_material: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        subject = self.subject

        version = self.version

        _algorithm: Optional[str] = None
        if self.algorithm is not None:
            _algorithm = self.algorithm.value

        encrypted_key_material = self.encrypted_key_material

        field_dict: Dict[str, Any] = {}
        if subject is not None:
            field_dict["subject"] = subject
        if version is not None:
            field_dict["version"] = version
        if _algorithm is not None:
            field_dict["algorithm"] = _algorithm
        if encrypted_key_material is not None:
            field_dict["encryptedKeyMaterial"] = encrypted_key_material

        return field_dict

    @classmethod
    def from_dict(cls: Type[T], src_dict: Dict[str, Any]) -> T:
        d = src_dict.copy()
        subject = d.pop("subject", None)

        version = d.pop("version", None)

        _algorithm = d.pop("algorithm", None)
        algorithm: Optional[DekAlgorithm]
        if _algorithm is None:
            algorithm = None
        else:
            algorithm = DekAlgorithm(_algorithm)

        encrypted_key_material = d.pop("encryptedKeyMaterial", None)

        create_dek_request = cls(  # type: ignore[call-arg]
            subject=subject,
            version=version,
            algorithm=algorithm,
            encrypted_key_material=encrypted_key_material,
        )

        return create_dek_request


@_attrs_define(frozen=True)
class KekId:
    name: str
    deleted: bool


@_attrs_define(frozen=True)
class DekId:
    kek_name: str
    subject: str
    version: int
    algorithm: DekAlgorithm
    deleted: bool


class _KekCache(object):
    def __init__(self):
        self.lock = Lock()
        self.keks = {}

    def set(self, kek_id: KekId, kek: Kek):
        with self.lock:
            self.keks[kek_id] = kek

    def get_kek(self, kek_id: KekId) -> Optional[Kek]:
        with self.lock:
            return self.keks.get(kek_id, None)

    def clear(self):
        with self.lock:
            self.keks.clear()


class _DekCache(object):
    def __init__(self):
        self.lock = Lock()
        self.deks = {}

    def set(self, dek_id: DekId, dek: Dek):
        with self.lock:
            self.deks[dek_id] = dek

    def get_dek_ids(self) -> List[DekId]:
        with self.lock:
            return list(self.deks.keys())

    def get_dek(self, dek_id: DekId) -> Optional[Dek]:
        with self.lock:
            return self.deks.get(dek_id, None)

    def remove(self, dek_id: DekId):
        with self.lock:
            if dek_id in self.deks:
                del self.deks[dek_id]

    def clear(self):
        with self.lock:
            self.deks.clear()


class DekRegistryClient(object):
    """
    A Confluent DEK Registry client.

    Args:
        conf (dict): DEK Registry client configuration.

    """  # noqa: E501

    def __init__(self, conf: dict):
        self._conf = conf
        self._rest_client = _RestClient(conf)
        self._kek_cache = _KekCache()
        self._dek_cache = _DekCache()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client.session.close()

    def config(self):
        return self._conf

    def register_kek(
        self,
        name: str,
        kms_type: str,
        kms_key_id: str,
        shared: bool = False,
        kms_props: Optional[Dict[str, str]] = None,
        doc: Optional[str] = None,
    ) -> Kek:
        """
        Register a new Key Encryption Key (KEK) with the DEK Registry.

        Args:
            name (str): Name of the KEK.
            kms_type (str): Type of the Key Management Service (KMS) used to manage the KEK.
            kms_key_id (str): Identifier of the KEK in the KMS.
            kms_props (Dict[str, str]): Additional properties for the KMS.
            doc (str): Description of the KEK.
            shared (bool): Whether the KEK is shared.

        Returns:
            Kek: KEK instance.

        Raises:
            SchemaRegistryError: If KEK can't be registered.
        """  # noqa: E501

        cache_key = KekId(name=name, deleted=False)
        kek = self._kek_cache.get_kek(cache_key)
        if kek is not None:
            return kek

        request = CreateKekRequest(
            name=name,
            kms_type=kms_type,
            kms_key_id=kms_key_id,
            kms_props=KekKmsProps.from_dict(kms_props) if kms_props is not None else None,
            doc=doc,
            shared=shared,
        )

        response = self._rest_client.post('/dek-registry/v1/keks', request.to_dict())
        kek = Kek.from_dict(response)

        self._kek_cache.set(cache_key, kek)

        return kek

    def get_kek(self, name: str, deleted: bool = False) -> Kek:
        """
        Get a Key Encryption Key (KEK) from the DEK Registry.

        Args:
            name (str): Name of the KEK.
            deleted (bool): Whether to include deleted KEKs.

        Returns:
            Kek: KEK instance.

        Raises:
            SchemaRegistryError: If KEK can't be found.
        """

        cache_key = KekId(name=name, deleted=deleted)
        kek = self._kek_cache.get_kek(cache_key)
        if kek is not None:
            return kek

        query = {'deleted': deleted}
        response = self._rest_client.get('/dek-registry/v1/keks/{}'.format(urllib.parse.quote(name, safe='')), query)
        kek = Kek.from_dict(response)

        self._kek_cache.set(cache_key, kek)

        return kek

    def register_dek(
        self,
        kek_name: str,
        subject: str,
        encrypted_key_material: str,
        algorithm: DekAlgorithm = DekAlgorithm.AES256_GCM,
        version: int = 1,
    ) -> Dek:
        """
        Register a new Data Encryption Key (DEK) with the DEK Registry.

        Args:
            kek_name (str): Name of the Key Encryption Key (KEK) used to encrypt the DEK.
            subject (str): Subject of the DEK.
            version (int): Version of the DEK.
            algorithm (DekAlgorithm): Algorithm used to encrypt the DEK.
            encrypted_key_material (str): Encrypted key material.

        Returns:
            Dek: DEK instance.

        Raises:
            SchemaRegistryError: If DEK can't be registered.
        """

        cache_key = DekId(kek_name=kek_name, subject=subject, version=version, algorithm=algorithm, deleted=False)
        dek = self._dek_cache.get_dek(cache_key)
        if dek is not None:
            return dek

        request = CreateDekRequest(
            subject=subject, version=version, algorithm=algorithm, encrypted_key_material=encrypted_key_material
        )

        dek = self._create_dek(kek_name, request)

        self._dek_cache.set(cache_key, dek)
        # Ensure latest dek is invalidated, such as in case of conflict (409)
        self._dek_cache.remove(
            DekId(kek_name=kek_name, subject=subject, version=-1, algorithm=algorithm, deleted=False)
        )
        self._dek_cache.remove(DekId(kek_name=kek_name, subject=subject, version=-1, algorithm=algorithm, deleted=True))

        return dek

    def _create_dek(self, kek_name: str, request: CreateDekRequest) -> Dek:
        from confluent_kafka.schema_registry.error import SchemaRegistryError

        if request.subject is None:
            raise TypeError("Subject cannot be None")
        try:
            # Try newer API with subject in the path
            path = '/dek-registry/v1/keks/{}/deks/{}'.format(
                urllib.parse.quote(kek_name), urllib.parse.quote(request.subject, safe='')
            )
            response = self._rest_client.post(path, request.to_dict())
            return Dek.from_dict(response)
        except SchemaRegistryError as e:
            if e.http_status_code == 405:
                # Try fallback to older API that does not have subject in the path
                path = '/dek-registry/v1/keks/{}/deks'.format(urllib.parse.quote(kek_name))
                response = self._rest_client.post(path, request.to_dict())
                return Dek.from_dict(response)
            else:
                raise

    def get_dek(
        self,
        kek_name: str,
        subject: str,
        algorithm: DekAlgorithm = DekAlgorithm.AES256_GCM,
        version: int = 1,
        deleted: bool = False,
    ) -> Dek:
        """
        Get a Data Encryption Key (DEK) from the DEK Registry.

        Args:
            kek_name (str): Name of the Key Encryption Key (KEK) used to encrypt the DEK.
            subject (str): Subject of the DEK.
            version (int): Version of the DEK.
            algorithm (DekAlgorithm): Algorithm used to encrypt the DEK.
            deleted (bool): Whether to include deleted DEKs.

        Returns:
            Dek: DEK instance.

        Raises:
            SchemaRegistryError: If DEK can't be found.
        """

        cache_key = DekId(kek_name=kek_name, subject=subject, version=version, algorithm=algorithm, deleted=deleted)
        dek = self._dek_cache.get_dek(cache_key)
        if dek is not None:
            return dek

        query = {'algorithm': algorithm, 'deleted': deleted}
        response = self._rest_client.get(
            '/dek-registry/v1/keks/{}/deks/{}/versions/{}'.format(
                urllib.parse.quote(kek_name), urllib.parse.quote(subject, safe=''), version
            ),
            query,
        )
        dek = Dek.from_dict(response)

        self._dek_cache.set(cache_key, dek)

        return dek

    @staticmethod
    def new_client(conf: dict) -> 'DekRegistryClient':
        from .mock_dek_registry_client import MockDekRegistryClient

        url = conf.get("url")
        if url is None:
            return MockDekRegistryClient({"url": "mock://"})
        if url.startswith("mock://"):
            return MockDekRegistryClient(conf)
        return DekRegistryClient(conf)
