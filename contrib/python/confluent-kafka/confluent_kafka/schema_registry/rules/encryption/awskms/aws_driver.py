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

import boto3
import tink
from botocore.credentials import DeferredRefreshableCredentials, create_assume_role_refresher
from tink import KmsClient
from tink.integration.awskms import new_client

from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, register_kms_driver

_PREFIX = "aws-kms://"
_ACCESS_KEY_ID = "access.key.id"
_SECRET_ACCESS_KEY = "secret.access.key"
_PROFILE = "profile"
_ROLE_ARN = "role.arn"
_ROLE_SESSION_NAME = "role.session.name"
_ROLE_EXTERNAL_ID = "role.external.id"


class AwsKmsDriver(KmsDriver):
    def __init__(self) -> None:
        pass

    def get_key_url_prefix(self) -> str:
        return _PREFIX

    def new_kms_client(self, conf: Dict[str, Any], key_url: Optional[str]) -> KmsClient:
        uri_prefix = _PREFIX
        if key_url is not None:
            uri_prefix = key_url

        role_arn = conf.get(_ROLE_ARN)
        if role_arn is None:
            role_arn = os.getenv("AWS_ROLE_ARN")
        role_session_name = conf.get(_ROLE_SESSION_NAME)
        if role_session_name is None:
            role_session_name = os.getenv("AWS_ROLE_SESSION_NAME")
        role_external_id = conf.get(_ROLE_EXTERNAL_ID)
        if role_external_id is None:
            role_external_id = os.getenv("AWS_ROLE_EXTERNAL_ID")
        role_web_identity_token_file = os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")
        key = conf.get(_ACCESS_KEY_ID)
        secret = conf.get(_SECRET_ACCESS_KEY)
        profile = conf.get(_PROFILE)

        key_arn = _key_uri_to_key_arn(uri_prefix)
        region = _get_region_from_key_arn(key_arn)
        if key is not None and secret is not None:
            session = boto3.Session(region_name=region, aws_access_key_id=key, aws_secret_access_key=secret)
        elif profile is not None:
            session = boto3.Session(
                region_name=region,
                profile_name=profile,
            )
        else:
            session = boto3.Session(region_name=region)
        # If role_web_identity_token_file is set, use the DefaultCredentialsProvider
        if role_arn is not None and role_web_identity_token_file is None:
            sts_client = session.client('sts')
            params = {
                'RoleArn': role_arn,
                'RoleSessionName': role_session_name if role_session_name is not None else 'confluent-encrypt',
            }
            if role_external_id is not None:
                params['ExternalId'] = role_external_id
            session._session._credentials = DeferredRefreshableCredentials(
                method='sts-assume-role',
                refresh_using=create_assume_role_refresher(
                    sts_client,
                    params,
                ),
            )
        client = session.client('kms')
        return new_client(boto3_client=client, key_uri=uri_prefix)

    @classmethod
    def register(cls) -> None:
        register_kms_driver(AwsKmsDriver())


def _key_uri_to_key_arn(key_uri: str) -> str:
    if not key_uri.startswith(_PREFIX):
        raise tink.TinkError('invalid key URI')
    return key_uri[len(_PREFIX) :]


def _get_region_from_key_arn(key_arn: str) -> str:
    # An AWS key ARN is of the form
    # arn:aws:kms:us-west-2:111122223333:key/1234abcd-12ab-34cd-56ef-1234567890ab.
    key_arn_parts = key_arn.split(':')
    if len(key_arn_parts) < 6:
        raise tink.TinkError('invalid key id')
    return key_arn_parts[3]
