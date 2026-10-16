#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2026 Confluent Inc.
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

import abc
from typing import Optional
from urllib.parse import urlparse, urlunparse

__all__ = [
    'normalize_identity_pool',
    '_AbstractOAuthBearerFieldProviderBuilder',
    '_AbstractOAuthBearerOIDCFieldProviderBuilder',
    '_StaticOAuthBearerFieldProviderBuilder',
    '_AbstractCustomOAuthBearerFieldProviderBuilder',
    '_AbstractOAuthBearerOIDCAzureIMDSFieldProviderBuilder',
    '_BearerFieldProvider',
    '_AsyncBearerFieldProvider',
    '_StaticFieldProvider',
]


def normalize_identity_pool(identity_pool_raw: "str | list[str] | None") -> Optional[str]:
    """
    Normalize identity pool configuration to a comma-separated string.

    Identity pool can be provided as:
    - None: Returns None (no identity pool configured)
    - str: Returns as-is (single pool ID or already comma-separated)
    - list[str]: Joins with commas (multiple pool IDs)

    Args:
        identity_pool_raw: The raw identity pool configuration value.

    Returns:
        A comma-separated string of identity pool IDs, or None.

    Raises:
        TypeError: If identity_pool_raw is not None, str, or list of strings.
    """
    if identity_pool_raw is None:
        return None
    if isinstance(identity_pool_raw, str):
        return identity_pool_raw
    if isinstance(identity_pool_raw, list):
        if not all(isinstance(item, str) for item in identity_pool_raw):
            raise TypeError("All items in identity pool list must be strings")
        return ",".join(identity_pool_raw)
    raise TypeError("identity pool id must be a str or list, not " + str(type(identity_pool_raw)))


class _BearerFieldProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_bearer_fields(self) -> dict:
        raise NotImplementedError


class _AsyncBearerFieldProvider(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    async def get_bearer_fields(self) -> dict:
        raise NotImplementedError


class _AbstractOAuthBearerFieldProviderBuilder(metaclass=abc.ABCMeta):
    """Abstract base class for OAuthBearer client builders"""

    required_properties = ['bearer.auth.logical.cluster']

    def __init__(self, conf: dict):
        self.conf: dict = conf
        self.logical_cluster: str = ""
        # identity pool is optional; may be omitted entirely
        self.identity_pool: Optional[str] = None

    def _validate(self):
        missing_properties = [
            prop for prop in _AbstractOAuthBearerFieldProviderBuilder.required_properties if prop not in self.conf
        ]
        if missing_properties:
            raise ValueError(
                "Missing required bearer configuration properties: {}".format(", ".join(missing_properties))
            )

        self.logical_cluster = self.conf.pop('bearer.auth.logical.cluster')
        if not isinstance(self.logical_cluster, str):
            raise TypeError("logical cluster must be a str, not " + str(type(self.logical_cluster)))

        # identity pool is optional and may be provided as a str or list of strings
        self.identity_pool = normalize_identity_pool(self.conf.pop('bearer.auth.identity.pool.id', None))

    @abc.abstractmethod
    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int) -> _BearerFieldProvider:
        pass


class _AbstractOAuthBearerOIDCFieldProviderBuilder(_AbstractOAuthBearerFieldProviderBuilder):
    required_properties = [
        'bearer.auth.client.id',
        'bearer.auth.client.secret',
        'bearer.auth.scope',
        'bearer.auth.issuer.endpoint.url',
    ]

    def __init__(self, conf: dict):
        super().__init__(conf)
        self.client_id: str = ""
        self.client_secret: str = ""
        self.scope: str = ""
        self.token_endpoint: str = ""

    def _validate(self):
        super()._validate()

        missing_properties = [
            prop for prop in _AbstractOAuthBearerOIDCFieldProviderBuilder.required_properties if prop not in self.conf
        ]
        if missing_properties:
            raise ValueError(
                "Missing required OAuth configuration properties: {}".format(", ".join(missing_properties))
            )

        self.client_id = self.conf.pop('bearer.auth.client.id')
        if not isinstance(self.client_id, str):
            raise TypeError("bearer.auth.client.id must be a str, not " + str(type(self.client_id)))

        self.client_secret = self.conf.pop('bearer.auth.client.secret')
        if not isinstance(self.client_secret, str):
            raise TypeError("bearer.auth.client.secret must be a str, not " + str(type(self.client_secret)))

        self.scope = self.conf.pop('bearer.auth.scope')
        if not isinstance(self.scope, str):
            raise TypeError("bearer.auth.scope must be a str, not " + str(type(self.scope)))

        self.token_endpoint = self.conf.pop('bearer.auth.issuer.endpoint.url')
        if not isinstance(self.token_endpoint, str):
            raise TypeError("bearer.auth.issuer.endpoint.url must be a str, not " + str(type(self.token_endpoint)))


class _AbstractOAuthBearerOIDCAzureIMDSFieldProviderBuilder(_AbstractOAuthBearerFieldProviderBuilder):

    def __init__(self, conf: dict):
        super().__init__(conf)
        self.token_endpoint: str = 'http://169.254.169.254/metadata/identity/oauth2/token'

    def _validate(self):
        super()._validate()

        token_endpoint_override = 'bearer.auth.issuer.endpoint.url' in self.conf
        self.token_endpoint = self.conf.pop('bearer.auth.issuer.endpoint.url', self.token_endpoint)
        if not isinstance(self.token_endpoint, str):
            raise TypeError("bearer.auth.issuer.endpoint.url must be a str, not " + str(type(self.token_endpoint)))

        try:
            parsed_token_endpoint = urlparse(self.token_endpoint)
        except Exception as ex:
            raise ValueError(f'Failed to parse token endpoint URL: {ex}')

        token_query = self.conf.pop('bearer.auth.issuer.endpoint.query', None)
        if token_query:
            if not isinstance(token_query, str):
                raise TypeError("bearer.auth.issuer.endpoint.query must be a str, not " + str(type(token_query)))

            parsed_token_endpoint = parsed_token_endpoint._replace(query=token_query, fragment=None)
            self.token_endpoint = urlunparse(parsed_token_endpoint)
        elif not token_endpoint_override:
            raise ValueError(
                "bearer.auth.issuer.endpoint.query must be provided "
                "when bearer.auth.issuer.endpoint.url isn't overridden"
            )


class _StaticFieldProvider(_BearerFieldProvider):
    def __init__(self, token: str, logical_cluster: str, identity_pool: Optional[str] = None):
        self.token: str = token
        self.logical_cluster: str = logical_cluster
        self.identity_pool: Optional[str] = identity_pool

    def get_bearer_fields(self) -> dict:
        fields = {
            'bearer.auth.token': self.token,
            'bearer.auth.logical.cluster': self.logical_cluster,
        }
        if self.identity_pool is not None:
            fields['bearer.auth.identity.pool.id'] = self.identity_pool
        return fields


class _StaticOAuthBearerFieldProviderBuilder(_AbstractOAuthBearerFieldProviderBuilder):

    def __init__(self, conf: dict):
        super().__init__(conf)
        self.static_token: str = ""

    def _validate(self):
        super()._validate()

        if 'bearer.auth.token' not in self.conf:
            raise ValueError("Missing bearer.auth.token")
        self.static_token = self.conf.pop('bearer.auth.token')
        if not isinstance(self.static_token, str):
            raise TypeError("bearer.auth.token must be a str, not " + str(type(self.static_token)))

    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self._validate()
        return _StaticFieldProvider(self.static_token, self.logical_cluster, self.identity_pool)


class _AbstractCustomOAuthBearerFieldProviderBuilder:
    required_properties = ['bearer.auth.custom.provider.function', 'bearer.auth.custom.provider.config']

    def __init__(self, conf: dict):
        self.conf = conf
        self.custom_function = None
        self.custom_config = None

    def _validate(self):
        missing_properties = [
            prop for prop in _AbstractCustomOAuthBearerFieldProviderBuilder.required_properties if prop not in self.conf
        ]
        if missing_properties:
            raise ValueError(
                "Missing required custom OAuth configuration properties: {}".format(", ".join(missing_properties))
            )

        self.custom_function = self.conf.pop('bearer.auth.custom.provider.function')
        if not callable(self.custom_function):
            raise TypeError(
                "bearer.auth.custom.provider.function must be a callable, not " + str(type(self.custom_function))
            )

        self.custom_config = self.conf.pop('bearer.auth.custom.provider.config')
        if not isinstance(self.custom_config, dict):
            raise TypeError("bearer.auth.custom.provider.config must be a dict, not " + str(type(self.custom_config)))
