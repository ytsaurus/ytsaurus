#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
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
import json
import logging
import os
import ssl
import threading as _locks
import time
import urllib
from typing import Any, Callable, Dict, List, Literal, Optional, Type, Union
from urllib.parse import unquote, urlparse

import certifi
import httpx
from authlib.integrations.httpx_client import OAuth2Client
from cachetools import Cache, LRUCache, TTLCache
from httpx import Response

from confluent_kafka import version
from confluent_kafka.schema_registry.common._oauthbearer import (
    _AbstractCustomOAuthBearerFieldProviderBuilder,
    _AbstractOAuthBearerOIDCAzureIMDSFieldProviderBuilder,
    _AbstractOAuthBearerOIDCFieldProviderBuilder,
    _BearerFieldProvider,
    _StaticOAuthBearerFieldProviderBuilder,
)
from confluent_kafka.schema_registry.common.schema_registry_client import (
    Association,
    AssociationCreateOrUpdateRequest,
    AssociationResponse,
    RegisteredSchema,
    Schema,
    SchemaVersion,
    ServerConfig,
    _SchemaCache,
    _StaticFieldProvider,
    full_jitter,
    is_retriable,
    is_success,
)
from confluent_kafka.schema_registry.error import OAuthTokenError, SchemaRegistryError

__all__ = [
    '_urlencode',
    '_CustomOAuthClient',
    '_OAuthClient',
    '_BaseRestClient',
    '_RestClient',
    'SchemaRegistryClient',
]

# TODO: consider adding `six` dependency or employing a compat file
# Python 2.7 is officially EOL so compatibility issue will be come more the norm.
# We need a better way to handle these issues.
# Six is one possibility but the compat file pattern used by requests
# is also quite nice.
#
# six: https://pypi.org/project/six/
# compat file : https://github.com/psf/requests/blob/master/requests/compat.py
try:
    string_type = basestring  # type: ignore[name-defined]  # noqa

    def _urlencode(value: str) -> str:
        return urllib.quote(value, safe='')  # type: ignore[attr-defined]

except NameError:
    string_type = str

    def _urlencode(value: str) -> str:
        return urllib.parse.quote(value, safe='')


log = logging.getLogger(__name__)


class _CustomOAuthClient(_BearerFieldProvider):
    def __init__(self, custom_function: Callable[[Dict], Dict], custom_config: dict):
        self.custom_function = custom_function
        self.custom_config = custom_config

    def get_bearer_fields(self) -> dict:
        return self.custom_function(self.custom_config)


class _AbstractOAuthClient(_BearerFieldProvider):
    def __init__(
        self,
        logical_cluster: str,
        identity_pool: Optional[str],
        max_retries: int,
        retries_wait_ms: int,
        retries_max_wait_ms: int,
    ):
        self.logical_cluster: str = logical_cluster
        self.identity_pool: Optional[str] = identity_pool
        self.max_retries: int = max_retries
        self.retries_wait_ms: int = retries_wait_ms
        self.retries_max_wait_ms: int = retries_max_wait_ms
        self.token: str = ""

    def get_bearer_fields(self) -> dict:
        fields = {
            'bearer.auth.token': self.get_access_token(),
            'bearer.auth.logical.cluster': self.logical_cluster,
        }
        if self.identity_pool is not None:
            fields['bearer.auth.identity.pool.id'] = self.identity_pool
        return fields

    def get_access_token(self) -> str:
        if not self.token or self.token_expired():
            self.generate_access_token()

        return self.token

    @abc.abstractmethod
    def token_expired(self) -> bool:
        raise NotImplementedError

    @abc.abstractmethod
    def fetch_token(self) -> str:
        raise NotImplementedError

    def generate_access_token(self) -> None:
        for i in range(self.max_retries + 1):
            try:
                self.token = self.fetch_token()
                return
            except Exception as e:
                if i >= self.max_retries:
                    raise OAuthTokenError(
                        f"Failed to retrieve token after {self.max_retries} " f"attempts due to error: {str(e)}"
                    )
                time.sleep(full_jitter(self.retries_wait_ms, self.retries_max_wait_ms, i) / 1000)


class _OAuthClient(_AbstractOAuthClient):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        scope: str,
        token_endpoint: str,
        logical_cluster: str,
        max_retries: int,
        retries_wait_ms: int,
        retries_max_wait_ms: int,
        identity_pool: Optional[str] = None,
    ):
        super().__init__(logical_cluster, identity_pool, max_retries, retries_wait_ms, retries_max_wait_ms)
        self.client = OAuth2Client(client_id=client_id, client_secret=client_secret, scope=scope)
        self.token_endpoint: str = token_endpoint
        self.token_object: dict = {}
        self.token_expiry_threshold: float = 0.8

    def token_expired(self) -> bool:
        expiry_window = self.token_object['expires_in'] * (1 - self.token_expiry_threshold)
        return self.token_object['expires_at'] < time.time() + expiry_window

    def fetch_token(self) -> str:
        self.token_object = self.client.fetch_token(url=self.token_endpoint, grant_type='client_credentials')
        return self.token_object['access_token']


class _OAuthAzureIMDSClient(_AbstractOAuthClient):
    def __init__(
        self,
        token_endpoint: str,
        logical_cluster: str,
        identity_pool: Optional[str],
        max_retries: int,
        retries_wait_ms: int,
        retries_max_wait_ms: int,
    ):
        super().__init__(logical_cluster, identity_pool, max_retries, retries_wait_ms, retries_max_wait_ms)
        self.client = httpx.Client()
        self.token_endpoint: str = token_endpoint
        self.token_object: dict = {}
        self.token_expiry_threshold: float = 0.8

    def token_expired(self) -> bool:
        expiry_window = int(self.token_object['expires_in']) * (1 - self.token_expiry_threshold)
        return int(self.token_object['expires_on']) < time.time() + expiry_window

    def fetch_token(self) -> str:
        self.token_object = (self.client.get(self.token_endpoint, headers=[('Metadata', 'true')])).json()
        return self.token_object['access_token']


class _OAuthBearerOIDCFieldProviderBuilder(_AbstractOAuthBearerOIDCFieldProviderBuilder):

    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self._validate()
        return _OAuthClient(
            self.client_id,
            self.client_secret,
            self.scope,
            self.token_endpoint,
            self.logical_cluster,
            max_retries,
            retries_wait_ms,
            retries_max_wait_ms,
            self.identity_pool,
        )


class _OAuthBearerOIDCAzureIMDSFieldProviderBuilder(_AbstractOAuthBearerOIDCAzureIMDSFieldProviderBuilder):

    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self._validate()
        return _OAuthAzureIMDSClient(
            self.token_endpoint,
            self.logical_cluster,
            self.identity_pool,
            max_retries,
            retries_wait_ms,
            retries_max_wait_ms,
        )


class _CustomOAuthBearerFieldProviderBuilder(_AbstractCustomOAuthBearerFieldProviderBuilder):

    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self._validate()
        assert self.custom_function is not None
        assert self.custom_config is not None
        return _CustomOAuthClient(self.custom_function, self.custom_config)


class _StaticFieldProviderBuilder(_StaticOAuthBearerFieldProviderBuilder):

    def build(self, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        self._validate()
        return _StaticFieldProvider(self.static_token, self.logical_cluster, self.identity_pool)


class _FieldProviderBuilder:

    __builders: Dict[str, Type[Any]] = {
        "OAUTHBEARER": _OAuthBearerOIDCFieldProviderBuilder,
        "OAUTHBEARER_AZURE_IMDS": _OAuthBearerOIDCAzureIMDSFieldProviderBuilder,
        "STATIC_TOKEN": _StaticFieldProviderBuilder,
        "CUSTOM": _CustomOAuthBearerFieldProviderBuilder,
    }

    @staticmethod
    def build(conf, max_retries: int, retries_wait_ms: int, retries_max_wait_ms: int):
        bearer_auth_credentials_source = conf.pop('bearer.auth.credentials.source', None)
        if bearer_auth_credentials_source is None:
            return [None, None]

        if bearer_auth_credentials_source not in _FieldProviderBuilder.__builders:
            raise ValueError('Unrecognized bearer.auth.credentials.source')
        bearer_field_provider_builder = _FieldProviderBuilder.__builders[bearer_auth_credentials_source](conf)
        return (
            bearer_auth_credentials_source,
            bearer_field_provider_builder.build(max_retries, retries_wait_ms, retries_max_wait_ms),
        )


class _BaseRestClient(object):

    def __init__(self, conf: dict):
        # copy dict to avoid mutating the original
        conf_copy = conf.copy()

        base_url = conf_copy.pop('url', None)
        if base_url is None:
            raise ValueError("Missing required configuration property url")
        if not isinstance(base_url, string_type):
            raise TypeError("url must be a str, not " + str(type(base_url)))
        base_urls = []
        for url in base_url.split(','):
            url = url.strip().rstrip('/')
            if not url.startswith('http') and not url.startswith('mock'):
                raise ValueError("Invalid url {}".format(url))
            base_urls.append(url)
        if not base_urls:
            raise ValueError("Missing required configuration property url")
        self.base_urls = base_urls

        ca: Union[str, bool, None] = conf_copy.pop('ssl.ca.location', None)
        key: Optional[str] = conf_copy.pop('ssl.key.location', None)
        key_password: Optional[str] = conf_copy.pop('ssl.key.password', None)
        client_cert: Optional[str] = conf_copy.pop('ssl.certificate.location', None)

        # this mimicks legacy, deprecated behaviour of httpx
        # self.verify is always set to an ssl.SSLContext in case we need to load_cert_chain
        if ca is False:
            self.verify = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            self.verify.check_hostname = False
            self.verify.verify_mode = ssl.CERT_NONE
        elif isinstance(ca, str):
            if os.path.isdir(ca):
                self.verify = ssl.create_default_context(capath=ca)
            else:
                self.verify = ssl.create_default_context(cafile=ca)
        else:
            if os.environ.get("SSL_CERT_FILE"):
                self.verify = ssl.create_default_context(cafile=os.environ["SSL_CERT_FILE"])
            elif os.environ.get("SSL_CERT_DIR"):
                self.verify = ssl.create_default_context(capath=os.environ["SSL_CERT_DIR"])
            else:
                self.verify = ssl.create_default_context(cafile=certifi.where())

        if client_cert is not None:
            if key is not None and key_password is not None:
                self.verify.load_cert_chain(certfile=client_cert, keyfile=key, password=key_password)
            elif key is not None:
                self.verify.load_cert_chain(certfile=client_cert, keyfile=key)
            elif key_password is not None:
                self.verify.load_cert_chain(certfile=client_cert, password=key_password)
            else:
                self.verify.load_cert_chain(certfile=client_cert)

        if (key is not None or key_password is not None) and client_cert is None:
            raise ValueError(
                "ssl.certificate.location required when" " configuring ssl.key.location or ssl.key.password"
            )

        parsed = urlparse(self.base_urls[0])
        try:
            userinfo = (unquote(parsed.username), unquote(parsed.password))
        except (AttributeError, TypeError):
            userinfo = ("", "")
        if 'basic.auth.user.info' in conf_copy:
            if userinfo != ('', ''):
                raise ValueError(
                    "basic.auth.user.info configured with"
                    " userinfo credentials in the URL."
                    " Remove userinfo credentials from the url or"
                    " remove basic.auth.user.info from the"
                    " configuration"
                )

            userinfo = tuple(conf_copy.pop('basic.auth.user.info', '').split(':', 1))

            if len(userinfo) != 2:
                raise ValueError("basic.auth.user.info must be in the form" " of {username}:{password}")

        self.auth = userinfo if userinfo != ('', '') else None

        # The following adds support for proxy config
        # If specified: it uses the specified proxy details when making requests
        self.proxy = None
        proxy = conf_copy.pop('proxy', None)
        if proxy is not None:
            self.proxy = proxy

        self.timeout = None
        timeout = conf_copy.pop('timeout', None)
        if timeout is not None:
            self.timeout = timeout

        self.cache_capacity = 1000
        cache_capacity = conf_copy.pop('cache.capacity', None)
        if cache_capacity is not None:
            if not isinstance(cache_capacity, (int, float)):
                raise TypeError("cache.capacity must be a number, not " + str(type(cache_capacity)))
            self.cache_capacity = int(cache_capacity)

        self.cache_latest_ttl_sec = None
        cache_latest_ttl_sec = conf_copy.pop('cache.latest.ttl.sec', None)
        if cache_latest_ttl_sec is not None:
            if not isinstance(cache_latest_ttl_sec, (int, float)):
                raise TypeError("cache.latest.ttl.sec must be a number, not " + str(type(cache_latest_ttl_sec)))
            self.cache_latest_ttl_sec = cache_latest_ttl_sec

        self.max_retries = 3
        max_retries = conf_copy.pop('max.retries', None)
        if max_retries is not None:
            if not isinstance(max_retries, (int, float)):
                raise TypeError("max.retries must be a number, not " + str(type(max_retries)))
            self.max_retries = int(max_retries)

        self.retries_wait_ms = 1000
        retries_wait_ms = conf_copy.pop('retries.wait.ms', None)
        if retries_wait_ms is not None:
            if not isinstance(retries_wait_ms, (int, float)):
                raise TypeError("retries.wait.ms must be a number, not " + str(type(retries_wait_ms)))
            self.retries_wait_ms = int(retries_wait_ms)

        self.retries_max_wait_ms = 20000
        retries_max_wait_ms = conf_copy.pop('retries.max.wait.ms', None)
        if retries_max_wait_ms is not None:
            if not isinstance(retries_max_wait_ms, (int, float)):
                raise TypeError("retries.max.wait.ms must be a number, not " + str(type(retries_max_wait_ms)))
            self.retries_max_wait_ms = int(retries_max_wait_ms)

        [self.bearer_auth_credentials_source, self.bearer_field_provider] = _FieldProviderBuilder.build(
            conf_copy, self.max_retries, self.retries_wait_ms, self.retries_max_wait_ms
        )

        # Any leftover keys are unknown to _RestClient
        if len(conf_copy) > 0:
            raise ValueError("Unrecognized properties: {}".format(", ".join(conf_copy.keys())))

    def get(self, url: str, query: Optional[dict] = None) -> Any:
        raise NotImplementedError()

    def post(self, url: str, body: Optional[dict], **kwargs) -> Any:
        raise NotImplementedError()

    def delete(self, url: str, query: Optional[dict] = None) -> Any:
        raise NotImplementedError()

    def put(self, url: str, body: Optional[dict] = None) -> Any:
        raise NotImplementedError()


class _RestClient(_BaseRestClient):
    """
    HTTP client for Confluent Schema Registry.

    See SchemaRegistryClient for configuration details.

    Args:
        conf (dict): Dictionary containing _RestClient configuration
    """

    def __init__(self, conf: dict):
        super().__init__(conf)

        self.session = httpx.Client(verify=self.verify, auth=self.auth, proxy=self.proxy, timeout=self.timeout)

    def handle_bearer_auth(self, headers: dict) -> None:
        if self.bearer_field_provider is None:
            raise ValueError("Bearer field provider is not set")
        bearer_fields = self.bearer_field_provider.get_bearer_fields()
        # Note: bearer.auth.identity.pool.id is optional; only token and logical.cluster are required
        required_fields = ['bearer.auth.token', 'bearer.auth.logical.cluster']

        missing_fields = []
        for field in required_fields:
            if field not in bearer_fields:
                missing_fields.append(field)

        if missing_fields:
            raise ValueError(
                "Missing required bearer auth fields, needs to be set in config or custom function: {}".format(
                    ", ".join(missing_fields)
                )
            )

        headers["Authorization"] = "Bearer {}".format(bearer_fields['bearer.auth.token'])
        headers['target-sr-cluster'] = bearer_fields['bearer.auth.logical.cluster']

        if 'bearer.auth.identity.pool.id' in bearer_fields:
            headers['Confluent-Identity-Pool-Id'] = bearer_fields['bearer.auth.identity.pool.id']

    def get(self, url: str, query: Optional[dict] = None) -> Any:
        return self.send_request(url, method='GET', query=query)

    def post(self, url: str, body: Optional[dict], **kwargs) -> Any:
        return self.send_request(url, method='POST', body=body)

    def delete(self, url: str, query: Optional[dict] = None) -> Any:
        return self.send_request(url, method='DELETE', query=query)

    def put(self, url: str, body: Optional[dict] = None) -> Any:
        return self.send_request(url, method='PUT', body=body)

    def send_request(self, url: str, method: str, body: Optional[dict] = None, query: Optional[dict] = None) -> Any:
        """
        Sends HTTP request to the SchemaRegistry, trying each base URL in turn.

        All unsuccessful attempts will raise a SchemaRegistryError with the
        response contents. In most cases this will be accompanied by a
        Schema Registry supplied error code.

        In the event the response is malformed an error_code of -1 will be used.

        Args:
            url (str): Request path

            method (str): HTTP method

            body (str): Request content

            query (dict): Query params to attach to the URL

        Returns:
            dict: Schema Registry response content.
        """

        headers = {
            'Accept': "application/vnd.schemaregistry.v1+json,"
            " application/vnd.schemaregistry+json,"
            " application/json"
        }

        body_str: Optional[str] = None
        if body is not None:
            body_str = json.dumps(body)
            headers = {
                'Content-Length': str(len(body_str)),
                'Content-Type': "application/vnd.schemaregistry.v1+json",
                'Confluent-Accept-Unknown-Properties': "true",
                'Confluent-Client-Version': f"python/{version()}",
            }

        headers['Confluent-Client-Version'] = f"python/{version()}"

        if self.bearer_auth_credentials_source:
            self.handle_bearer_auth(headers)

        response = None
        for i, base_url in enumerate(self.base_urls):
            try:
                response = self.send_http_request(base_url, url, method, headers, body_str, query)

                if is_success(response.status_code):
                    if response.status_code == 204 or not response.content:
                        return None
                    return response.json()

                if not is_retriable(response.status_code) or i == len(self.base_urls) - 1:
                    break
            except Exception as e:
                if i == len(self.base_urls) - 1:
                    # Raise the exception since we have no more urls to try
                    raise e

        if isinstance(response, Response):
            try:
                raise SchemaRegistryError(
                    response.status_code, response.json().get('error_code'), response.json().get('message')
                )
            except (ValueError, KeyError, AttributeError):
                raise SchemaRegistryError(
                    response.status_code, -1, "Unknown Schema Registry Error: " + str(response.content)
                )
        else:
            raise TypeError("Unexpected response of unsupported type: " + str(type(response)))

    def send_http_request(
        self,
        base_url: str,
        url: str,
        method: str,
        headers: Optional[dict],
        body: Optional[str] = None,
        query: Optional[dict] = None,
    ) -> Response:
        """
        Sends a single HTTP request to the Schema Registry, retrying transient
        failures.

        Retries (up to max.retries, with exponential backoff) are attempted on
        retriable HTTP status codes and on network-level errors
        (httpx.TransportError: DNS failures, connection refused/reset, timeouts,
        etc.). The HTTP response is returned as-is, including error responses;
        converting an unsuccessful status into a SchemaRegistryError is done by
        the caller (send_request).

        Args:
            base_url (str): Schema Registry base URL

            url (str): Request path

            method (str): HTTP method

            headers (dict): Headers

            body (str): Request content

            query (dict): Query params to attach to the URL

        Returns:
            Response: The HTTP response, which may represent an error status.

        Raises:
            httpx.TransportError: If a network-level error persists after all
                retries are exhausted.
        """
        response = None
        for i in range(self.max_retries + 1):
            try:
                response = self.session.request(
                    method,
                    url="/".join([base_url.rstrip("/"), url.lstrip("/")]),
                    headers=headers,
                    content=body,
                    params=query,
                )
            except httpx.TransportError:
                # A TransportError means the request failed before a response
                # was received (DNS failure, connection refused/reset, timeout,
                # TLS error, etc.). Once retries are exhausted, re-raise so the
                # caller can fail over to the next URL.
                if i >= self.max_retries:
                    raise
                time.sleep(full_jitter(self.retries_wait_ms, self.retries_max_wait_ms, i) / 1000)
                continue

            if is_success(response.status_code):
                return response

            if not is_retriable(response.status_code) or i >= self.max_retries:
                return response

            time.sleep(full_jitter(self.retries_wait_ms, self.retries_max_wait_ms, i) / 1000)
        return response  # type: ignore[return-value]


class SchemaRegistryClient(object):
    """
    A Confluent Schema Registry client.

    Configuration properties (* indicates a required field):

    +------------------------------+------+-------------------------------------------------+
    | Property name                | type | Description                                     |
    +==============================+======+=================================================+
    | ``url`` *                    | str  | Comma-separated list of Schema Registry URLs.   |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to CA certificate file used                |
    | ``ssl.ca.location``          | str  | to verify the Schema Registry's                 |
    |                              |      | private key.                                    |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to client's private key                    |
    |                              |      | (PEM) used for authentication.                  |
    | ``ssl.key.location``         | str  |                                                 |
    |                              |      | ``ssl.certificate.location`` must also be set.  |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Password to use to decrypt the client's private |
    |                              |      | key.                                            |
    |                              |      |                                                 |
    | ``ssl.key.password``         | str  | The private key may be provided using           |
    |                              |      | ``ssl.key.location``, or bundled with the       |
    |                              |      | certificate in ``ssl.certificate.location``.    |
    |                              |      | Password is optional (key may be unencrypted).  |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Path to client's certificate (PEM) used for     |
    |                              |      | authentication.                                 |
    | ``ssl.certificate.location`` | str  |                                                 |
    |                              |      | May be set without ``ssl.key.location`` if the  |
    |                              |      | private key is stored within the PEM as well.   |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Client HTTP credentials in the form of          |
    |                              |      | ``username:password``.                          |
    | ``basic.auth.user.info``     | str  |                                                 |
    |                              |      | By default userinfo is extracted from           |
    |                              |      | the URL if present.                             |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``proxy``                    | str  | Proxy such as http://localhost:8030.            |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``timeout``                  | int  | Request timeout.                                |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``cache.capacity``           | int  | Cache capacity.  Defaults to 1000.              |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``cache.latest.ttl.sec``     | int  | TTL in seconds for caching the latest schema.   |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      |                                                 |
    | ``max.retries``              | int  | Maximum retries for a request.  Defaults to 2.  |
    |                              |      |                                                 |
    +------------------------------+------+-------------------------------------------------+
    |                              |      | Maximum time to wait for the first retry.       |
    |                              |      | When jitter is applied, the actual wait may     |
    | ``retries.wait.ms``          | int  | be less.                                        |
    |                              |      |                                                 |
    |                              |      | Defaults to 1000.                               |
    +------------------------------+------+-------------------------------------------------+

    Args:
        conf (dict): Schema Registry client configuration.

    See Also:
        `Confluent Schema Registry documentation <http://confluent.io/docs/current/schema-registry/docs/intro.html>`_
    """  # noqa: E501

    def __init__(self, conf: dict):
        self._conf = conf
        self._rest_client = _RestClient(conf)
        self._cache = _SchemaCache()
        self._latest_lock = _locks.Lock()
        cache_capacity = self._rest_client.cache_capacity
        cache_ttl = self._rest_client.cache_latest_ttl_sec
        self._latest_version_cache: Cache[Any, Any]
        self._latest_with_metadata_cache: Cache[Any, Any]
        if cache_ttl is not None:
            self._latest_version_cache = TTLCache(cache_capacity, cache_ttl)
            self._latest_with_metadata_cache = TTLCache(cache_capacity, cache_ttl)
        else:
            self._latest_version_cache = LRUCache(cache_capacity)
            self._latest_with_metadata_cache = LRUCache(cache_capacity)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        if self._rest_client is not None:
            self._rest_client.session.close()

    def config(self):
        return self._conf

    def register_schema(self, subject_name: str, schema: 'Schema', normalize_schemas: bool = False) -> int:
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under
            schema (Schema): Schema instance to register
            normalize_schemas (bool): Normalize schema before registering

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        See Also:
            `POST Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        registered_schema = self.register_schema_full_response(
            subject_name, schema, normalize_schemas=normalize_schemas
        )
        return registered_schema.schema_id  # type: ignore[return-value]

    def register_schema_full_response(
        self, subject_name: str, schema: 'Schema', normalize_schemas: bool = False
    ) -> 'RegisteredSchema':
        """
        Registers a schema under ``subject_name``.

        Args:
            subject_name (str): subject to register a schema under
            schema (Schema): Schema instance to register
            normalize_schemas (bool): Normalize schema before registering

        Returns:
            int: Schema id

        Raises:
            SchemaRegistryError: if Schema violates this subject's
                Compatibility policy or is otherwise invalid.

        See Also:
            `POST Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        schema_id = self._cache.get_id_by_schema(subject_name, schema)
        if schema_id is not None:
            result = self._cache.get_schema_by_id(subject_name, schema_id)
            if result is not None:
                return RegisteredSchema(
                    schema_id=schema_id, guid=result[0], subject=subject_name, version=None, schema=result[1]
                )

        request = schema.to_dict()

        response = self._rest_client.post(
            'subjects/{}/versions?normalize={}'.format(_urlencode(subject_name), normalize_schemas), body=request
        )

        response_schema = RegisteredSchema.from_dict(response)

        registered_schema = RegisteredSchema(
            schema_id=response_schema.schema_id,
            guid=response_schema.guid,
            subject=response_schema.subject or subject_name,
            version=response_schema.version,
            schema=response_schema.schema,
        )

        # The registered schema may not be fully populated
        s = registered_schema.schema if registered_schema.schema.schema_str is not None else schema
        self._cache.set_schema(subject_name, registered_schema.schema_id, registered_schema.guid, s)

        return registered_schema

    def get_schema(
        self,
        schema_id: int,
        subject_name: Optional[str] = None,
        fmt: Optional[str] = None,
        reference_format: Optional[str] = None,
    ) -> 'Schema':
        """
        Fetches the schema associated with ``schema_id`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional round-trip to the Schema Registry.

        Args:
            schema_id (int): Schema id.
            subject_name (str): Subject name the schema is registered under.
            fmt (str): Desired output format, dependent on schema type.
            reference_format (str): Desired output format for references.

        Returns:
            Schema: Schema instance identified by the ``schema_id``

        Raises:
            SchemaRegistryError: If schema can't be found.

        See Also:
            `GET Schema API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id>`_
        """  # noqa: E501

        result = self._cache.get_schema_by_id(subject_name, schema_id)
        if result is not None:
            return result[1]

        query = {}
        if subject_name is not None:
            query['subject'] = subject_name
        if fmt is not None:
            query['format'] = fmt
        if reference_format is not None:
            query['reference_format'] = reference_format
        response = self._rest_client.get('schemas/ids/{}'.format(schema_id), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_schema(subject_name, schema_id, registered_schema.guid, registered_schema.schema)
        if subject_name is not None:
            self._cache.set_registered_schema(registered_schema.schema, registered_schema)

        return registered_schema.schema

    def get_schema_by_guid(self, guid: str, fmt: Optional[str] = None) -> 'Schema':
        """
        Fetches the schema associated with ``guid`` from the
        Schema Registry. The result is cached so subsequent attempts will not
        require an additional round-trip to the Schema Registry.

        Args:
            guid (str): Schema guid
            fmt (str): Format of the schema

        Returns:
            Schema: Schema instance identified by the ``guid``

        Raises:
            SchemaRegistryError: If schema can't be found.

        See Also:
         `GET Schema API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id>`_
        """  # noqa: E501

        schema = self._cache.get_schema_by_guid(guid)
        if schema is not None:
            return schema

        query = {}
        if fmt is not None:
            query['format'] = fmt
        response = self._rest_client.get('schemas/guids/{}'.format(guid), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_schema(None, registered_schema.schema_id, registered_schema.guid, registered_schema.schema)

        return registered_schema.schema

    def get_schema_types(self) -> List[str]:
        """
        Lists all supported schema types in the Schema Registry.

        Returns:
            list(str): List of supported schema types (e.g., ['AVRO', 'JSON', 'PROTOBUF'])

        Raises:
            SchemaRegistryError: if schema types can't be retrieved

        See Also:
            `GET Schema Types API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-types>`_
        """  # noqa: E501

        return self._rest_client.get('schemas/types')

    def get_subjects_by_schema_id(
        self,
        schema_id: int,
        subject_name: Optional[str] = None,
        deleted: bool = False,
        offset: int = 0,
        limit: int = -1,
    ) -> List[str]:
        """
        Retrieves all the subjects associated with ``schema_id``.

        Args:
            schema_id (int): Schema ID.
            subject_name (str): Subject name that results can be filtered by.
            deleted (bool): Whether to include subjects where the schema was deleted.
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            list(str): List of subjects matching the specified parameters.

        Raises:
            SchemaRegistryError: if subjects can't be found
        """
        query: dict[str, Any] = {'offset': offset, 'limit': limit}
        if subject_name is not None:
            query['subject'] = subject_name
        if deleted:
            query['deleted'] = deleted
        return self._rest_client.get('schemas/ids/{}/subjects'.format(schema_id), query)

    def get_schema_versions(
        self,
        schema_id: int,
        subject_name: Optional[str] = None,
        deleted: bool = False,
        offset: int = 0,
        limit: int = -1,
    ) -> List[SchemaVersion]:
        """
        Gets all subject-version pairs of a schema by its ID.

        Args:
            schema_id (int): Schema ID.
            subject_name (str): Subject name that results can be filtered by.
            deleted (bool): Whether to include subject versions where the schema was deleted.
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            list(SchemaVersion): List of subject-version pairs. Each pair contains:
                - subject (str): Subject name.
                - version (int): Version number.

        Raises:
            SchemaRegistryError: if schema versions can't be found.

        See Also:
            `GET Schema Versions API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--schemas-ids-int-%20id-versions>`_
        """  # noqa: E501

        query: dict[str, Any] = {'offset': offset, 'limit': limit}
        if subject_name is not None:
            query['subject'] = subject_name
        if deleted:
            query['deleted'] = deleted
        response = self._rest_client.get('schemas/ids/{}/versions'.format(schema_id), query)
        return [SchemaVersion.from_dict(item) for item in response]

    def lookup_schema(
        self,
        subject_name: str,
        schema: 'Schema',
        normalize_schemas: bool = False,
        fmt: Optional[str] = None,
        deleted: bool = False,
    ) -> 'RegisteredSchema':
        """
        Returns ``schema`` registration information for ``subject``.

        Args:
            subject_name (str): Subject name the schema is registered under.
            schema (Schema): Schema instance.
            normalize_schemas (bool): Normalize schema before registering.
            fmt (str): Desired output format, dependent on schema type.
            deleted (bool): Whether to include deleted schemas.

        Returns:
            RegisteredSchema: Subject registration information for this schema.

        Raises:
            SchemaRegistryError: If schema or subject can't be found.

        See Also:
            `POST Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--subjects-(string-%20subject)>`_
        """  # noqa: E501

        registered_schema = self._cache.get_registered_by_subject_schema(subject_name, schema)
        if registered_schema is not None:
            return registered_schema

        request = schema.to_dict()

        query_params: dict[str, Any] = {'normalize': normalize_schemas, 'deleted': deleted}
        if fmt is not None:
            query_params['format'] = fmt

        query_string = '&'.join(f"{key}={value}" for key, value in query_params.items())

        response = self._rest_client.post('subjects/{}?{}'.format(_urlencode(subject_name), query_string), body=request)

        result = RegisteredSchema.from_dict(response)

        # Ensure the schema matches the input
        registered_schema = RegisteredSchema(
            schema_id=result.schema_id,
            guid=result.guid,
            subject=result.subject,
            version=result.version,
            schema=schema,
        )

        self._cache.set_registered_schema(schema, registered_schema)

        return registered_schema

    def get_subjects(
        self,
        subject_prefix: Optional[str] = None,
        deleted: bool = False,
        deleted_only: bool = False,
        offset: int = 0,
        limit: int = -1,
    ) -> List[str]:
        """
        Lists all subjects registered with the Schema Registry.

        Args:
            subject_prefix (str): Subject name prefix that results can be filtered by.
            deleted (bool): Whether to include deleted subjects.
            deleted_only (bool): Whether to return deleted subjects only. If both deleted and deleted_only are True, deleted_only takes precedence.
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            list(str): Registered subject names

        Raises:
            SchemaRegistryError: if subjects can't be found

        See Also:
            `GET subjects API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects>`_
        """  # noqa: E501

        query: dict[str, Any] = {'deleted': deleted, 'deleted_only': deleted_only, 'offset': offset, 'limit': limit}
        if subject_prefix is not None:
            query['subject'] = subject_prefix
        return self._rest_client.get('subjects', query)

    def delete_subject(self, subject_name: str, permanent: bool = False) -> List[int]:
        """
        Deletes the specified subject and its associated compatibility level if
        registered. It is recommended to use this API only when a topic needs
        to be recycled or in development environments.

        Args:
            subject_name (str): subject name
            permanent (bool): True for a hard delete, False (default) for a soft delete

        Returns:
            list(int): Versions deleted under this subject

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `DELETE Subject API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)>`_
        """  # noqa: E501

        if permanent:
            versions = self._rest_client.delete('subjects/{}?permanent=true'.format(_urlencode(subject_name)))
            self._cache.remove_by_subject(subject_name)
        else:
            versions = self._rest_client.delete('subjects/{}'.format(_urlencode(subject_name)))

        return versions

    def get_latest_version(self, subject_name: str, fmt: Optional[str] = None) -> 'RegisteredSchema':
        """
        Retrieves latest registered version for subject

        Args:
            subject_name (str): Subject name.
            fmt (str): Format of the schema

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        with self._latest_lock:
            registered_schema = self._latest_version_cache.get(subject_name, None)
        if registered_schema is not None:
            return registered_schema

        query = {'format': fmt} if fmt is not None else None
        response = self._rest_client.get('subjects/{}/versions/{}'.format(_urlencode(subject_name), 'latest'), query)

        registered_schema = RegisteredSchema.from_dict(response)

        with self._latest_lock:
            self._latest_version_cache[subject_name] = registered_schema

        return registered_schema

    def get_latest_with_metadata(
        self, subject_name: str, metadata: Dict[str, str], deleted: bool = False, fmt: Optional[str] = None
    ) -> 'RegisteredSchema':
        """
        Retrieves latest registered version for subject with the given metadata

        Args:
            subject_name (str): Subject name.
            metadata (dict): The key-value pairs for the metadata.
            deleted (bool): Whether to include deleted schemas.
            fmt (str): Format of the schema

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.
        """  # noqa: E501

        cache_key = (subject_name, frozenset(metadata.items()), deleted)
        with self._latest_lock:
            registered_schema = self._latest_with_metadata_cache.get(cache_key, None)
        if registered_schema is not None:
            return registered_schema

        query: dict[str, Any] = {'deleted': deleted}
        if fmt is not None:
            query['format'] = fmt
        keys = metadata.keys()
        if keys:
            query['key'] = [_urlencode(key) for key in keys]
            query['value'] = [_urlencode(metadata[key]) for key in keys]

        response = self._rest_client.get('subjects/{}/metadata'.format(_urlencode(subject_name)), query)

        registered_schema = RegisteredSchema.from_dict(response)

        with self._latest_lock:
            self._latest_with_metadata_cache[cache_key] = registered_schema

        return registered_schema

    def get_version(
        self,
        subject_name: str,
        version: Union[int, Literal["latest"]] = "latest",
        deleted: bool = False,
        fmt: Optional[str] = None,
    ) -> 'RegisteredSchema':
        """
        Retrieves a specific schema registered under `subject_name` and `version`.

        Args:
            subject_name (str): Subject name.
            version (Union[int, Literal["latest"]]): Version of the schema or string "latest". Defaults to latest version.
            deleted (bool): Whether to include deleted schemas.
            fmt (str): Format of the schema.

        Returns:
            RegisteredSchema: Registration information for this version.

        Raises:
            SchemaRegistryError: if the version can't be found or is invalid.

        See Also:
            `GET Subject Versions API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        if version != "latest":  # Skip cache lookup for reading the latest version
            registered_schema = self._cache.get_registered_by_subject_version(subject_name, version)
            if registered_schema is not None:
                return registered_schema

        query: dict[str, Any] = {'deleted': deleted, 'format': fmt} if fmt is not None else {'deleted': deleted}
        response = self._rest_client.get('subjects/{}/versions/{}'.format(_urlencode(subject_name), version), query)

        registered_schema = RegisteredSchema.from_dict(response)

        self._cache.set_registered_schema(registered_schema.schema, registered_schema)

        return registered_schema

    def get_referenced_by(
        self, subject_name: str, version: Union[int, Literal["latest"]] = "latest", offset: int = 0, limit: int = -1
    ) -> List[int]:
        """
        Get a list of IDs of schemas that reference the schema with the given `subject_name` and `version`.

        Args:
            subject_name (str): Subject name
            version (Union[int, Literal["latest"]]): Version number or "latest"
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            list(int): List of schema IDs that reference the specified schema.

        Raises:
            SchemaRegistryError: if the schema version can't be found or referenced schemas can't be retrieved

        See Also:
            `GET Subject Versions (ReferenceBy) API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions-versionId-%20version-referencedby>`_
        """  # noqa: E501

        query: dict[str, Any] = {'offset': offset, 'limit': limit}
        return self._rest_client.get(
            'subjects/{}/versions/{}/referencedby'.format(_urlencode(subject_name), version), query
        )

    def get_versions(
        self, subject_name: str, deleted: bool = False, deleted_only: bool = False, offset: int = 0, limit: int = -1
    ) -> List[int]:
        """
        Get a list of all versions registered with this subject.

        Args:
            subject_name (str): Subject name.
            deleted (bool): Whether to include deleted schemas.
            deleted_only (bool): Whether to return deleted versions only. If both deleted and deleted_only are True, deleted_only takes precedence.
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            list(int): Registered versions

        Raises:
            SchemaRegistryError: If subject can't be found

        See Also:
            `GET Subject All Versions API Reference <https://docs.confluent.io/platform/current/schema-registry/develop/api.html#get--subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        query: dict[str, Any] = {'deleted': deleted, 'deleted_only': deleted_only, 'offset': offset, 'limit': limit}
        return self._rest_client.get('subjects/{}/versions'.format(_urlencode(subject_name)), query)

    def delete_version(self, subject_name: str, version: int, permanent: bool = False) -> int:
        """
        Deletes a specific version registered to ``subject_name``.

        Args:
            subject_name (str) Subject name

            version (int): Version number

            permanent (bool): True for a hard delete, False (default) for a soft delete

        Returns:
            int: Version number which was deleted

        Raises:
            SchemaRegistryError: if the subject or version cannot be found.

        See Also:
            `Delete Subject Version API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        if permanent:
            response = self._rest_client.delete(
                'subjects/{}/versions/{}?permanent=true'.format(_urlencode(subject_name), version)
            )
        else:
            response = self._rest_client.delete('subjects/{}/versions/{}'.format(_urlencode(subject_name), version))

        # Clear cache for both soft and hard deletes to maintain consistency
        self._cache.remove_by_subject_version(subject_name, version)

        return response

    def set_compatibility(self, subject_name: Optional[str] = None, level: Optional[str] = None) -> str:
        """
        Update global or subject level compatibility level.

        Args:
            level (str): Compatibility level. See API reference for a list of
                valid values.

            subject_name (str, optional): Subject to update. Sets global compatibility
                level policy if not set.

        Returns:
            str: The newly configured compatibility level.

        Raises:
            SchemaRegistryError: If the compatibility level is invalid.

        See Also:
            `PUT Subject Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--config-(string-%20subject)>`_
        """  # noqa: E501

        if level is None:
            raise ValueError("level must be set")

        if subject_name is None:
            return self._rest_client.put('config', body={'compatibility': level.upper()})

        return self._rest_client.put(
            'config/{}'.format(_urlencode(subject_name)), body={'compatibility': level.upper()}
        )

    def get_compatibility(self, subject_name: Optional[str] = None) -> str:
        """
        Get the current compatibility level.

        Args:
            subject_name (str, optional): Subject name. Returns global policy
                if left unset.

        Returns:
            str: Compatibility level for the subject if set, otherwise the global compatibility level.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Subject Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--config-(string-%20subject)>`_
        """  # noqa: E501

        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'

        result = self._rest_client.get(url)
        return result['compatibilityLevel']

    def test_compatibility(
        self,
        subject_name: str,
        schema: 'Schema',
        version: Union[int, str] = "latest",
        normalize: bool = False,
        verbose: bool = False,
    ) -> bool:
        """
        Test the compatibility of a candidate schema for a given subject and version

        Args:
            subject_name (str): Subject name the schema is registered under
            schema (Schema): Schema instance.
            version (int or str, optional): Version number, or the string "latest". Defaults to "latest".
            normalize (bool): Whether to normalize the input schema.
            verbose (bool): Whether to return detailed error messages.

        Returns:
            bool: True if the schema is compatible with the specified version

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `POST Test Compatibility API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--compatibility-subjects-(string-%20subject)-versions-(versionId-%20version)>`_
        """  # noqa: E501

        request = schema.to_dict()
        response = self._rest_client.post(
            'compatibility/subjects/{}/versions/{}?normalize={}&verbose={}'.format(
                _urlencode(subject_name), version, normalize, verbose
            ),
            body=request,
        )
        return response['is_compatible']

    def test_compatibility_all_versions(
        self, subject_name: str, schema: 'Schema', normalize: bool = False, verbose: bool = False
    ) -> bool:
        """
        Test the input schema against all schema versions under the subject (depending on the compatibility level set).

        Args:
            subject_name (str): Subject of the schema versions against which compatibility is to be tested.
            schema (Schema): Schema instance.
            normalize (bool): Whether to normalize the input schema.
            verbose (bool): Whether to return detailed error messages.

        Returns:
            bool: True if the schema is compatible with all of the subject's schemas versions.
        See Also:
            `POST Test Compatibility Against All API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#post--compatibility-subjects-(string-%20subject)-versions>`_
        """  # noqa: E501

        request = schema.to_dict()
        response = self._rest_client.post(
            'compatibility/subjects/{}/versions?normalize={}&verbose={}'.format(
                _urlencode(subject_name), normalize, verbose
            ),
            body=request,
        )
        return response['is_compatible']

    def set_config(self, subject_name: Optional[str] = None, config: Optional['ServerConfig'] = None) -> 'ServerConfig':
        """
        Update global or subject config.

        Args:
            config (ServerConfig): Config. See API reference for a list of
                valid values.

            subject_name (str, optional): Subject to update. Sets global config
                if not set.

        Returns:
            str: The newly configured config.

        Raises:
            SchemaRegistryError: If the config is invalid.

        See Also:
            `PUT Subject Config API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--config-(string-%20subject)>`_
        """  # noqa: E501

        if config is None:
            raise ValueError("config must be set")

        if subject_name is None:
            return self._rest_client.put('config', body=config.to_dict())

        return self._rest_client.put('config/{}'.format(_urlencode(subject_name)), body=config.to_dict())

    def delete_config(self, subject_name: Optional[str] = None) -> 'ServerConfig':
        """
        Delete the specified subject-level compatibility level config and revert to the global default.

        Args:
            subject_name (str, optional): Subject name. Deletes global config
                if left unset.

        Returns:
            ServerConfig: The old deleted config

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `DELETE Subject Config API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--config-(string- subject)>`_
        """  # noqa: E501
        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'
        result = self._rest_client.delete(url)
        return ServerConfig.from_dict(result)

    def get_config(self, subject_name: Optional[str] = None) -> 'ServerConfig':
        """
        Get the current config.

        Args:
            subject_name (str, optional): Subject name. Returns global config
                if left unset.

        Returns:
            ServerConfig: Config for the subject if set, otherwise the global config.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Subject Config API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--config-(string-%20subject)>`_
        """  # noqa: E501

        if subject_name is not None:
            url = 'config/{}'.format(_urlencode(subject_name))
        else:
            url = 'config'

        result = self._rest_client.get(url)
        return ServerConfig.from_dict(result)

    def get_mode(self, subject_name: str) -> str:
        """
        Get the mode for a subject.

        Args:
            subject_name (str): Subject name.

        Returns:
            str: Mode for the subject. Returns one of IMPORT, READONLY, READWRITE (default).

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Subject Mode API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--mode-(string-%20subject)>`_
        """  # noqa: E501
        result = self._rest_client.get('mode/{}'.format(_urlencode(subject_name)))
        return result['mode']

    def update_mode(self, subject_name: str, mode: str, force: bool = False) -> str:
        """
        Update the mode for a subject.

        Args:
            subject_name (str): Subject name.
            mode (str): Mode to update.
            force (bool): Whether to force a mode change even if the Schema Registry has existing schemas.

        Returns:
            str: New mode for the subject. Must be one of IMPORT, READONLY, READWRITE (default).

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `PUT Subject Mode API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--mode-(string-%20subject)>`_
        """  # noqa: E501
        result = self._rest_client.put(
            'mode/{}?force={}'.format(_urlencode(subject_name), force),
            body={'mode': mode},
        )
        return result['mode']

    def delete_mode(self, subject_name: str) -> str:
        """
        Delete the mode for a subject and revert to the global default

        Args:
            subject_name (str): Subject name.

        Returns:
            str: New mode for the subject. Must be one of IMPORT, READONLY, READWRITE (default).

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `DELETE Subject Mode API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#delete--mode-(string-%20subject)>`_
        """  # noqa: E501
        result = self._rest_client.delete('mode/{}'.format(_urlencode(subject_name)))
        return result['mode']

    def get_global_mode(self) -> str:
        """
        Get the current mode for Schema Registry at a global level.

        Returns:
            str: Schema Registry mode. Must be one of IMPORT, READONLY, READWRITE (default).

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `GET Global Mode API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#get--mode>`_
        """  # noqa: E501
        result = self._rest_client.get('mode')
        return result['mode']

    def update_global_mode(self, mode: str, force: bool = False) -> str:
        """
        Update the mode for the Schema Registry at a global level.

        Args:
            mode (str): Mode to update.
            force (bool): Whether to force a mode change even if the Schema Registry has existing schemas.

        Returns:
            str: New mode for the Schema Registry. Must be one of IMPORT, READONLY, READWRITE (default).

        Raises:
            SchemaRegistryError: if the request was unsuccessful.

        See Also:
            `PUT Global Mode API Reference <https://docs.confluent.io/current/schema-registry/develop/api.html#put--mode>`_
        """  # noqa: E501
        result = self._rest_client.put('mode?force={}'.format(force), body={'mode': mode})
        return result['mode']

    def get_contexts(self, offset: int = 0, limit: int = -1) -> List[str]:
        """
        Retrieves a list of contexts.

        Args:
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            List[str]: List of contexts.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.
        """  # noqa: E501

        result = self._rest_client.get('contexts', query={'offset': offset, 'limit': limit})
        return result

    def clear_latest_caches(self):
        with self._latest_lock:
            self._latest_version_cache.clear()
            self._latest_with_metadata_cache.clear()

    def clear_caches(self):
        with self._latest_lock:
            self._latest_version_cache.clear()
            self._latest_with_metadata_cache.clear()
        self._cache.clear()

    def get_associations_by_resource_name(
        self,
        resource_name: str,
        resource_namespace: str,
        resource_type: Optional[str] = None,
        association_types: Optional[List[str]] = None,
        offset: int = 0,
        limit: int = -1,
    ) -> List['Association']:
        """
        Retrieves associations for a given resource name and namespace.

        Args:
            resource_name (str): The name of the resource (e.g., topic name).
            resource_namespace (str): The namespace of the resource (e.g., kafka cluster ID).
                Use "-" as a wildcard.
            resource_type (str, optional): The type of resource (e.g., "topic").
            association_types (List[str], optional): The types of associations to filter by
                (e.g., ["key", "value"]).
            offset (int): Pagination offset for results.
            limit (int): Pagination size for results. Ignored if negative.

        Returns:
            List[Association]: List of associations matching the criteria.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.
        """
        query: Dict[str, Any] = {}
        if resource_type is not None:
            query['resourceType'] = resource_type
        if association_types is not None:
            query['associationType'] = association_types
        if offset > 0:
            query['offset'] = offset
        if limit >= 1:
            query['limit'] = limit

        response = self._rest_client.get(
            'associations/resources/{}/{}'.format(_urlencode(resource_namespace), _urlencode(resource_name)), query
        )

        return [Association.from_dict(a) for a in response]

    def create_association(self, request: 'AssociationCreateOrUpdateRequest') -> 'AssociationResponse':
        """
        Creates an association between a subject and a resource.

        Args:
            request (AssociationCreateOrUpdateRequest): The association create or update request.

        Returns:
            AssociationResponse: The response containing the created associations.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.
        """
        response = self._rest_client.post('associations', body=request.to_dict())
        return AssociationResponse.from_dict(response)

    def delete_associations(
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
            association_types (List[str], optional): The types of associations to delete
                (e.g., ["key", "value"]). If not specified, all associations are deleted.
            cascade_lifecycle (bool): Whether to cascade the lifecycle policy to dependent schemas.

        Raises:
            SchemaRegistryError: if the request was unsuccessful.
        """
        query: Dict[str, Any] = {'cascadeLifecycle': cascade_lifecycle}
        if resource_type is not None:
            query['resourceType'] = resource_type
        if association_types is not None:
            query['associationType'] = association_types

        self._rest_client.delete('associations/resources/{}'.format(_urlencode(resource_id)), query=query)

    @staticmethod
    def new_client(conf: dict) -> 'SchemaRegistryClient':
        from .mock_schema_registry_client import MockSchemaRegistryClient

        url = conf.get("url")
        if url and isinstance(url, str) and url.startswith("mock://"):
            return MockSchemaRegistryClient(conf)
        return SchemaRegistryClient(conf)
