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
import uuid
from collections import defaultdict
from threading import Lock
from typing import Dict, List, Literal, Optional, Union

from ..common.schema_registry_client import (
    Association,
    AssociationCreateOrUpdateRequest,
    AssociationInfo,
    AssociationResponse,
    RegisteredSchema,
    Schema,
    ServerConfig,
)
from ..error import SchemaRegistryError
from .schema_registry_client import AsyncSchemaRegistryClient


class _SchemaStore(object):

    def __init__(self):
        self.lock = Lock()
        self.max_id = 0
        self.schema_id_index = {}
        self.schema_guid_index = {}
        self.schema_index = {}
        self.subject_schemas = defaultdict(set)

    def set(self, registered_schema: RegisteredSchema) -> RegisteredSchema:
        with self.lock:
            self.max_id += 1
            rs = RegisteredSchema(
                schema_id=self.max_id,
                guid=registered_schema.guid,
                schema=registered_schema.schema,
                subject=registered_schema.subject,
                version=registered_schema.version,
            )
            self.schema_id_index[rs.schema_id] = rs
            self.schema_guid_index[rs.guid] = rs
            self.schema_index[rs.schema] = rs.schema_id
            self.subject_schemas[rs.subject].add(rs)
            return rs

    def get_schema(self, schema_id: int) -> Optional[Schema]:
        with self.lock:
            rs = self.schema_id_index.get(schema_id, None)
            return rs.schema if rs else None

    def get_schema_by_guid(self, guid: str) -> Optional[Schema]:
        with self.lock:
            rs = self.schema_guid_index.get(guid, None)
            return rs.schema if rs else None

    def get_registered_schema_by_schema(self, subject_name: str, schema: Schema) -> Optional[RegisteredSchema]:
        with self.lock:
            if subject_name in self.subject_schemas:
                for rs in self.subject_schemas[subject_name]:
                    if rs.schema == schema:
                        return rs
            return None

    def get_version(self, subject_name: str, version: Union[int, str]) -> Optional[RegisteredSchema]:
        with self.lock:
            if subject_name in self.subject_schemas:
                for rs in self.subject_schemas[subject_name]:
                    if rs.version == version:
                        return rs
            return None

    def get_latest_version(self, subject_name: str) -> Optional[RegisteredSchema]:
        with self.lock:
            if subject_name in self.subject_schemas:
                latest_version = 0
                latest_schema = None
                for rs in self.subject_schemas[subject_name]:
                    if rs.version > latest_version:
                        latest_version = rs.version
                        latest_schema = rs
                return latest_schema
            return None

    def get_latest_with_metadata(
        self, subject_name: str, metadata: Dict[str, str], deleted: bool = False, fmt: Optional[str] = None
    ) -> Optional[RegisteredSchema]:
        with self.lock:
            if subject_name in self.subject_schemas:
                rs: RegisteredSchema
                for rs in self.subject_schemas[subject_name]:
                    if (
                        rs.schema
                        and rs.schema.metadata
                        and rs.schema.metadata.properties
                        and metadata.items() <= rs.schema.metadata.properties.properties.items()
                    ):
                        return rs
            return None

    def get_subjects(self) -> List[str]:
        with self.lock:
            return list(self.subject_schemas.keys())

    def get_versions(self, subject_name: str) -> List[int]:
        with self.lock:
            if subject_name in self.subject_schemas:
                return [rs.version for rs in self.subject_schemas[subject_name]]
            return []

    def remove_by_schema(self, registered_schema: RegisteredSchema):
        with self.lock:
            subject_name = registered_schema.subject
            if subject_name in self.subject_schemas:
                self.subject_schemas[subject_name].remove(registered_schema)

    def remove_by_subject(self, subject_name: str) -> List[int]:
        with self.lock:
            versions = []
            if subject_name in self.subject_schemas:
                for rs in self.subject_schemas[subject_name]:
                    versions.append(rs.version)
                    schema_id = self.schema_index.pop(rs.schema, None)
                    if schema_id is not None:
                        self.schema_id_index.pop(schema_id, None)

                del self.subject_schemas[subject_name]
            return versions

    def clear(self):
        with self.lock:
            self.schema_id_index.clear()
            self.schema_guid_index.clear()
            self.schema_index.clear()
            self.subject_schemas.clear()


class _AssociationStore(object):

    def __init__(self) -> None:
        self.lock = Lock()
        # Key: resource_id -> List[Association]
        self.associations_by_resource_id: Dict[str, List[Association]] = defaultdict(list)
        # Key: (resource_namespace, resource_name) -> resource_id
        self.resource_id_index: Dict[tuple, str] = {}

    def create_association(self, request: AssociationCreateOrUpdateRequest) -> AssociationResponse:
        with self.lock:
            resource_id = request.resource_id
            resource_name = request.resource_name
            resource_namespace = request.resource_namespace
            resource_type = request.resource_type

            # Index resource_id by (namespace, name)
            if resource_name and resource_namespace and resource_id:
                self.resource_id_index[(resource_namespace, resource_name)] = resource_id

            created_associations = []
            if request.associations and resource_id is not None:
                for assoc_info in request.associations:
                    association = Association(
                        subject=assoc_info.subject,
                        guid=None,
                        resource_name=resource_name,
                        resource_namespace=resource_namespace,
                        resource_id=resource_id,
                        resource_type=resource_type,
                        association_type=assoc_info.association_type,
                        frozen=assoc_info.frozen if assoc_info.frozen is not None else False,
                    )
                    self.associations_by_resource_id[resource_id].append(association)
                    created_associations.append(
                        AssociationInfo(
                            subject=assoc_info.subject,
                            association_type=assoc_info.association_type,
                            lifecycle=assoc_info.lifecycle,
                            frozen=assoc_info.frozen if assoc_info.frozen is not None else False,
                            schema=assoc_info.schema,
                        )
                    )

            return AssociationResponse(
                resource_name=resource_name,
                resource_namespace=resource_namespace,
                resource_id=resource_id,
                resource_type=resource_type,
                associations=created_associations,
            )

    def delete_associations(
        self,
        resource_id: str,
        resource_type: Optional[str] = None,
        association_types: Optional[List[str]] = None,
    ) -> None:
        with self.lock:
            if resource_id not in self.associations_by_resource_id:
                return

            if association_types is None and resource_type is None:
                # Delete all associations for this resource
                del self.associations_by_resource_id[resource_id]
            else:
                # Filter and keep only non-matching associations
                remaining = []
                for assoc in self.associations_by_resource_id[resource_id]:
                    keep = False
                    if resource_type is not None and assoc.resource_type != resource_type:
                        keep = True
                    if association_types is not None and assoc.association_type not in association_types:
                        keep = True
                    if keep:
                        remaining.append(assoc)
                self.associations_by_resource_id[resource_id] = remaining

    def get_associations_by_resource_name(
        self,
        resource_name: str,
        resource_namespace: str,
        resource_type: Optional[str] = None,
        association_types: Optional[List[str]] = None,
    ) -> List[Association]:
        with self.lock:
            result = []
            for resource_id, associations in self.associations_by_resource_id.items():
                for assoc in associations:
                    # Check if namespace matches (or is wildcard)
                    if resource_namespace != "-" and assoc.resource_namespace != resource_namespace:
                        continue
                    if assoc.resource_name != resource_name:
                        continue
                    if resource_type is not None and assoc.resource_type != resource_type:
                        continue
                    if association_types is not None and assoc.association_type not in association_types:
                        continue
                    result.append(assoc)
            return result

    def clear(self):
        with self.lock:
            self.associations_by_resource_id.clear()
            self.resource_id_index.clear()


class AsyncMockSchemaRegistryClient(AsyncSchemaRegistryClient):

    def __init__(self, conf: dict):
        super().__init__(conf)
        self._store = _SchemaStore()
        self._association_store = _AssociationStore()

    async def register_schema(self, subject_name: str, schema: 'Schema', normalize_schemas: bool = False) -> int:
        registered_schema = await self.register_schema_full_response(
            subject_name, schema, normalize_schemas=normalize_schemas
        )
        return registered_schema.schema_id  # type: ignore[return-value]

    async def register_schema_full_response(
        self, subject_name: str, schema: 'Schema', normalize_schemas: bool = False
    ) -> 'RegisteredSchema':
        registered_schema = self._store.get_registered_schema_by_schema(subject_name, schema)
        if registered_schema is not None:
            return registered_schema

        latest_schema = self._store.get_latest_version(subject_name)
        latest_version = 1 if latest_schema is None or latest_schema.version is None else latest_schema.version + 1

        registered_schema = RegisteredSchema(
            schema_id=1, guid=str(uuid.uuid4()), schema=schema, subject=subject_name, version=latest_version
        )

        registered_schema = self._store.set(registered_schema)

        return registered_schema

    async def get_schema(
        self,
        schema_id: int,
        subject_name: Optional[str] = None,
        fmt: Optional[str] = None,
        reference_format: Optional[str] = None,
    ) -> 'Schema':
        schema = self._store.get_schema(schema_id)
        if schema is not None:
            return schema

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def get_schema_by_guid(self, guid: str, fmt: Optional[str] = None) -> 'Schema':
        schema = self._store.get_schema_by_guid(guid)
        if schema is not None:
            return schema

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def lookup_schema(
        self,
        subject_name: str,
        schema: 'Schema',
        normalize_schemas: bool = False,
        fmt: Optional[str] = None,
        deleted: bool = False,
    ) -> 'RegisteredSchema':

        registered_schema = self._store.get_registered_schema_by_schema(subject_name, schema)
        if registered_schema is not None:
            return registered_schema

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def get_subjects(
        self,
        subject_prefix: Optional[str] = None,
        deleted: bool = False,
        deleted_only: bool = False,
        offset: int = 0,
        limit: int = -1,
    ) -> List[str]:
        """
        Note: Mock implementation does not support deleted/deleted_only parameters
        as the mock does not track deletion state.
        """
        subjects = self._store.get_subjects()

        # Filter by prefix if provided
        if subject_prefix is not None:
            subjects = [s for s in subjects if s.startswith(subject_prefix)]

        # Apply pagination
        if offset > 0:
            subjects = subjects[offset:]
        if limit >= 0:
            subjects = subjects[:limit]

        return subjects

    async def delete_subject(self, subject_name: str, permanent: bool = False) -> List[int]:
        return self._store.remove_by_subject(subject_name)

    async def get_latest_version(self, subject_name: str, fmt: Optional[str] = None) -> 'RegisteredSchema':
        registered_schema = self._store.get_latest_version(subject_name)
        if registered_schema is not None:
            return registered_schema

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def get_latest_with_metadata(
        self, subject_name: str, metadata: Dict[str, str], deleted: bool = False, fmt: Optional[str] = None
    ) -> 'RegisteredSchema':
        registered_schema = self._store.get_latest_with_metadata(subject_name, metadata)
        if registered_schema is not None:
            return registered_schema

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def get_version(
        self,
        subject_name: str,
        version: Union[int, Literal["latest"]] = "latest",
        deleted: bool = False,
        fmt: Optional[str] = None,
    ) -> 'RegisteredSchema':
        if version == "latest":
            registered_schema = self._store.get_latest_version(subject_name)
        else:
            registered_schema = self._store.get_version(subject_name, version)
        if registered_schema is not None:
            return registered_schema

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def get_versions(
        self, subject_name: str, deleted: bool = False, deleted_only: bool = False, offset: int = 0, limit: int = -1
    ) -> List[int]:
        """
        Note: Mock implementation does not support deleted/deleted_only parameters
        as the mock does not track deletion state.
        """
        versions = self._store.get_versions(subject_name)

        # Apply pagination
        if offset > 0:
            versions = versions[offset:]
        if limit >= 0:
            versions = versions[:limit]

        return versions

    async def delete_version(self, subject_name: str, version: int, permanent: bool = False) -> int:
        registered_schema = self._store.get_version(subject_name, version)
        if registered_schema is not None:
            self._store.remove_by_schema(registered_schema)
            return registered_schema.schema_id  # type: ignore[return-value]

        raise SchemaRegistryError(404, 40400, "Schema Not Found")

    async def set_config(
        self, subject_name: Optional[str] = None, config: Optional['ServerConfig'] = None  # noqa F821
    ) -> 'ServerConfig':  # noqa F821
        return None  # type: ignore[return-value]

    async def get_config(self, subject_name: Optional[str] = None) -> 'ServerConfig':  # noqa F821
        return None  # type: ignore[return-value]

    async def get_associations_by_resource_name(
        self,
        resource_name: str,
        resource_namespace: str,
        resource_type: Optional[str] = None,
        association_types: Optional[List[str]] = None,
        offset: int = 0,
        limit: int = -1,
    ) -> List['Association']:
        return self._association_store.get_associations_by_resource_name(
            resource_name, resource_namespace, resource_type, association_types
        )

    async def create_association(self, request: 'AssociationCreateOrUpdateRequest') -> 'AssociationResponse':
        """
        Creates an association between a subject and a resource.

        Args:
            request (AssociationCreateOrUpdateRequest): The association create or update request.

        Returns:
            AssociationResponse: The response containing the created associations.
        """
        return self._association_store.create_association(request)

    async def delete_associations(
        self,
        resource_id: str,
        resource_type: Optional[str] = None,
        association_types: Optional[List[str]] = None,
        cascade_lifecycle: bool = False,
    ) -> None:
        """
        Deletes associations for a resource.

        Args:
            resource_id (str): The resource identifier.
            resource_type (str, optional): The type of resource (e.g., "topic").
            association_types (List[str], optional): The types of associations to delete.
            cascade_lifecycle (bool): Whether to cascade the lifecycle policy to dependent schemas.
        """
        self._association_store.delete_associations(resource_id, resource_type, association_types)
