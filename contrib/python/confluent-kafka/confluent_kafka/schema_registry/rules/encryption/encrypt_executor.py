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

import base64
import io
import logging
import time
from typing import Any, List, Optional, Tuple

from tink import KmsClient, TinkError, aead, daead, kms_client_from_uri, register_kms_client
from tink.core import Registry
from tink.proto import aes_siv_pb2, tink_pb2

from confluent_kafka.schema_registry import _MAGIC_BYTE_V0, RuleMode, SchemaRegistryError
from confluent_kafka.schema_registry.rule_registry import RuleRegistry
from confluent_kafka.schema_registry.rules.encryption.dek_registry.dek_registry_client import (
    Dek,
    DekAlgorithm,
    DekId,
    DekRegistryClient,
    Kek,
    KekId,
)
from confluent_kafka.schema_registry.rules.encryption.kms_driver_registry import KmsDriver, get_kms_driver
from confluent_kafka.schema_registry.serde import (
    FieldContext,
    FieldRuleExecutor,
    FieldTransform,
    FieldType,
    RuleContext,
    RuleError,
    RuleExecutor,
)

log = logging.getLogger(__name__)


aead.register()
daead.register()

ENCRYPT_KEK_NAME = "encrypt.kek.name"
ENCRYPT_KMS_KEY_ID = "encrypt.kms.key.id"
ENCRYPT_KMS_TYPE = "encrypt.kms.type"
ENCRYPT_DEK_ALGORITHM = "encrypt.dek.algorithm"
ENCRYPT_DEK_EXPIRY_DAYS = "encrypt.dek.expiry.days"
ENCRYPT_ALTERNATE_KMS_KEY_IDS = "encrypt.alternate.kms.key.ids"

MILLIS_IN_DAY = 24 * 60 * 60 * 1000


class Clock(object):
    def now(self) -> int:
        return int(round(time.time() * 1000))


class EncryptionExecutor(RuleExecutor):

    def __init__(self, clock: Clock = Clock()):
        self.client: Optional[DekRegistryClient] = None
        self.config: Optional[dict] = None
        self.clock = clock

    def configure(self, client_conf: dict, rule_conf: dict):
        if client_conf:
            if self.client:
                if self.client.config() != client_conf:
                    raise RuleError("executor already configured")
            else:
                self.client = DekRegistryClient.new_client(client_conf)

        if self.config:
            if rule_conf:
                for key, value in rule_conf.items():
                    v = self.config.get(key)
                    if v is not None:
                        if v != value:
                            raise RuleError(f"rule config key already set: {key}")
                    else:
                        self.config[key] = value
        else:
            self.config = rule_conf if rule_conf else {}

    def type(self) -> str:
        return "ENCRYPT_PAYLOAD"

    def transform(self, ctx: RuleContext, message: Any) -> Any:
        executor = self.new_transform(ctx)
        return executor.transform(ctx, FieldType.BYTES, message)

    def new_transform(self, ctx: RuleContext) -> 'EncryptionExecutorTransform':
        cryptor = self._get_cryptor(ctx)
        kek_name = self._get_kek_name(ctx)
        dek_expiry_days = self._get_dek_expiry_days(ctx)
        transform = EncryptionExecutorTransform(self, cryptor, kek_name, dek_expiry_days)
        return transform

    def close(self):
        if self.client is not None:
            self.client.__exit__()

    def _get_cryptor(self, ctx: RuleContext) -> 'Cryptor':
        dek_algorithm = DekAlgorithm.AES256_GCM
        dek_algorithm_str = ctx.get_parameter(ENCRYPT_DEK_ALGORITHM)
        if dek_algorithm_str is not None:
            dek_algorithm = DekAlgorithm[dek_algorithm_str]
        cryptor = Cryptor(dek_algorithm)
        return cryptor

    def _get_kek_name(self, ctx: RuleContext) -> str:
        kek_name = ctx.get_parameter(ENCRYPT_KEK_NAME)
        if kek_name is None:
            raise RuleError("no kek name found")
        if kek_name == "":
            raise RuleError("empty kek name")
        return kek_name

    def _get_dek_expiry_days(self, ctx: RuleContext) -> int:
        dek_expiry_days_str = ctx.get_parameter(ENCRYPT_DEK_EXPIRY_DAYS)
        if dek_expiry_days_str is None:
            return 0
        try:
            dek_expiry_days = int(dek_expiry_days_str)
        except ValueError:
            raise RuleError("invalid expiry days")
        if dek_expiry_days < 0:
            raise RuleError("negative expiry days")
        return dek_expiry_days

    @classmethod
    def register(cls):
        RuleRegistry.register_rule_executor(EncryptionExecutor())

    @classmethod
    def register_with_clock(cls, clock: Clock) -> 'EncryptionExecutor':
        executor = EncryptionExecutor(clock)
        RuleRegistry.register_rule_executor(executor)
        return executor


class Cryptor:
    EMPTY_AAD = b""

    def __init__(self, dek_format: DekAlgorithm):
        self.dek_format = dek_format
        self.is_deterministic = dek_format == DekAlgorithm.AES256_SIV
        self.registry = Registry()

        if dek_format is DekAlgorithm.AES128_GCM:
            self.key_template = aead.aead_key_templates.AES128_GCM_RAW
        elif dek_format is DekAlgorithm.AES256_GCM:
            self.key_template = aead.aead_key_templates.AES256_GCM_RAW
        elif dek_format is DekAlgorithm.AES256_SIV:
            # Construct AES256_SIV_RAW since it doesn't exist in Tink
            key_format = aes_siv_pb2.AesSivKeyFormat(
                # Generate 2 256-bit keys
                key_size=64,
            )
            self.key_template = tink_pb2.KeyTemplate(
                type_url=daead.deterministic_aead_key_templates.AES256_SIV.type_url,
                output_prefix_type=tink_pb2.RAW,
                value=key_format.SerializeToString(),
            )
        else:
            raise RuleError("invalid dek algorithm")

    def generate_key(self) -> bytes:
        key_data = self.registry.new_key_data(self.key_template)
        return key_data.value

    def encrypt(self, dek: bytes, plaintext: bytes, associated_data: bytes) -> bytes:
        key_data = tink_pb2.KeyData(
            type_url=self.key_template.type_url, value=dek, key_material_type=tink_pb2.KeyData.SYMMETRIC
        )
        if self.is_deterministic:
            primitive = self.registry.primitive(key_data, daead.DeterministicAead)
            return primitive.encrypt_deterministically(plaintext, associated_data)
        else:
            primitive = self.registry.primitive(key_data, aead.Aead)
            return primitive.encrypt(plaintext, associated_data)

    def decrypt(self, dek: bytes, ciphertext: bytes, associated_data: bytes) -> bytes:
        key_data = tink_pb2.KeyData(
            type_url=self.key_template.type_url, value=dek, key_material_type=tink_pb2.KeyData.SYMMETRIC
        )
        if self.is_deterministic:
            primitive = self.registry.primitive(key_data, daead.DeterministicAead)
            return primitive.decrypt_deterministically(ciphertext, associated_data)
        else:
            primitive = self.registry.primitive(key_data, aead.Aead)
            return primitive.decrypt(ciphertext, associated_data)


class EncryptionExecutorTransform(object):

    def __init__(self, executor: EncryptionExecutor, cryptor: Cryptor, kek_name: str, dek_expiry_days: int):
        self._executor = executor
        self._cryptor = cryptor
        self._kek_name = kek_name
        self._kek: Optional[Kek] = None
        self._dek_expiry_days = dek_expiry_days

    def _is_dek_rotated(self):
        return self._dek_expiry_days > 0

    def _get_kek(self, ctx: RuleContext) -> Kek:
        if self._kek is None:
            self._kek = self._get_or_create_kek(ctx)
        return self._kek

    def _get_or_create_kek(self, ctx: RuleContext) -> Kek:
        is_read = ctx.rule_mode == RuleMode.READ
        kms_type = ctx.get_parameter(ENCRYPT_KMS_TYPE)
        kms_key_id = ctx.get_parameter(ENCRYPT_KMS_KEY_ID)
        kek_id = KekId(self._kek_name, False)
        kek = self._retrieve_kek_from_registry(kek_id)
        if kek is None:
            if is_read:
                raise RuleError(f"no kek found for {self._kek_name} during consume")
            if not kms_type:
                raise RuleError(f"no kms type found for {self._kek_name} during produce")
            if not kms_key_id:
                raise RuleError(f"no kms key id found for {self._kek_name} during produce")
            kek = self._store_kek_to_registry(kek_id, kms_type, kms_key_id, False)
            if kek is None:
                # handle conflicts (409)
                kek = self._retrieve_kek_from_registry(kek_id)
            if kek is None:
                raise RuleError(f"no kek found for {self._kek_name} during produce")
        if kms_type and kek.kms_type != kms_type:
            raise RuleError(
                f"found {self._kek_name} with kms type {kek.kms_type} " f"which differs from rule kms type {kms_type}"
            )
        if kms_key_id and kek.kms_key_id != kms_key_id:
            raise RuleError(
                f"found {self._kek_name} with kms key id {kek.kms_key_id} "
                f"which differs from rule kms key id {kms_key_id}"
            )
        return kek

    def _retrieve_kek_from_registry(self, kek_id: KekId) -> Optional[Kek]:
        if self._executor.client is None:
            raise RuleError("client not configured")
        try:
            return self._executor.client.get_kek(kek_id.name, kek_id.deleted)
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 404:
                return None
            raise RuleError(f"could not get kek {kek_id.name}") from e

    def _store_kek_to_registry(self, kek_id: KekId, kms_type: str, kms_key_id: str, shared: bool) -> Optional[Kek]:
        if self._executor.client is None:
            raise RuleError("client not configured")
        try:
            return self._executor.client.register_kek(kek_id.name, kms_type, kms_key_id, shared)
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 409:
                return None
            raise RuleError(f"could not register kek {kek_id.name}") from e

    def _get_or_create_dek(self, ctx: RuleContext, version: Optional[int]) -> Dek:
        kek = self._get_kek(ctx)
        is_read = ctx.rule_mode == RuleMode.READ
        if version is None or version == 0:
            version = 1
        # TODO: fallback value for name?
        dek_id = DekId(kek.name, ctx.subject, version, self._cryptor.dek_format, is_read)  # type: ignore[arg-type]
        dek = self._retrieve_dek_from_registry(dek_id)
        is_expired = self._is_expired(ctx, dek)
        primitive = None
        if dek is None or is_expired:
            if is_read:
                raise RuleError(f"no dek found for {dek_id.kek_name} during consume")
            if self._kek is None:
                raise RuleError("no kek found")
            encrypted_dek = None
            if not kek.shared:
                if self._executor.config is None:
                    raise RuleError("config not found in executor")
                primitive = AeadWrapper(self._executor.config, self._kek)
                raw_dek = self._cryptor.generate_key()
                encrypted_dek = primitive.encrypt(raw_dek, self._cryptor.EMPTY_AAD)
            if dek is None or dek.version is None:
                new_version = 1
            else:
                new_version = dek.version + 1 if is_expired else 1
            try:
                dek = self._create_dek(dek_id, new_version, encrypted_dek)
            except RuleError as e:
                if dek is None:
                    raise e
                log.warning(
                    "failed to create dek for %s, subject %s, version %d, using existing dek",
                    kek.name,
                    ctx.subject,
                    new_version,
                )
        key_bytes = dek.get_key_material_bytes()
        if key_bytes is None:
            if primitive is None:
                primitive = AeadWrapper(self._executor.config, self._kek)  # type: ignore[arg-type]
            encrypted_dek = dek.get_encrypted_key_material_bytes()
            raw_dek = primitive.decrypt(encrypted_dek, self._cryptor.EMPTY_AAD)  # type: ignore[arg-type]
            dek.set_key_material(raw_dek)
        return dek

    def _create_dek(self, dek_id: DekId, new_version: Optional[int], encrypted_dek: Optional[bytes]) -> Dek:
        # TODO: fallback value for version?
        new_dek_id = DekId(
            dek_id.kek_name,
            dek_id.subject,
            new_version,  # type: ignore[arg-type]
            dek_id.algorithm,
            dek_id.deleted,
        )
        dek = self._store_dek_to_registry(new_dek_id, encrypted_dek)
        if dek is None:
            # handle conflicts (409)
            dek = self._retrieve_dek_from_registry(dek_id)
        if dek is None:
            raise RuleError(f"no dek found for {dek_id.kek_name} during produce")
        return dek

    def _retrieve_dek_from_registry(self, key: DekId) -> Optional[Dek]:
        try:
            version = key.version
            if not version:
                version = 1
            if self._executor.client is None:
                raise RuleError("client not configured")
            dek = self._executor.client.get_dek(key.kek_name, key.subject, key.algorithm, version, key.deleted)
            return dek if dek and dek.encrypted_key_material else None
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 404:
                return None
            raise RuleError(f"could not get dek for kek {key.kek_name}, subject {key.subject}") from e

    def _store_dek_to_registry(self, key: DekId, encrypted_dek: Optional[bytes]) -> Optional[Dek]:
        try:
            encrypted_dek_str = base64.b64encode(encrypted_dek).decode("utf-8") if encrypted_dek else None
            if self._executor.client is None:
                raise RuleError("client not configured")
            dek = self._executor.client.register_dek(
                key.kek_name, key.subject, encrypted_dek_str, key.algorithm, key.version  # type: ignore[arg-type]
            )
            return dek
        except Exception as e:
            if isinstance(e, SchemaRegistryError) and e.http_status_code == 409:
                return None
            raise RuleError(f"could not register dek for kek {key.kek_name}, subject {key.subject}") from e

    def _is_expired(self, ctx: RuleContext, dek: Optional[Dek]) -> bool:
        now = self._executor.clock.now()
        return (
            ctx.rule_mode != RuleMode.READ
            and self._dek_expiry_days > 0
            and dek is not None
            and (now - (dek.ts or 0)) / MILLIS_IN_DAY > self._dek_expiry_days
        )  # type: ignore[operator]

    def transform(self, ctx: RuleContext, field_type: FieldType, field_value: Any) -> Any:
        if field_value is None:
            return None
        if ctx.rule_mode == RuleMode.WRITE:
            plaintext = self._to_bytes(field_type, field_value)
            if plaintext is None:
                raise RuleError(f"type {field_type} not supported for encryption")
            version = None
            if self._is_dek_rotated():
                version = -1
            dek = self._get_or_create_dek(ctx, version)
            key_material_bytes = dek.get_key_material_bytes()
            if key_material_bytes is None:
                raise RuleError("no key material bytes found for dek")
            ciphertext = self._cryptor.encrypt(key_material_bytes, plaintext, Cryptor.EMPTY_AAD)
            if self._is_dek_rotated():
                if dek.version is None:
                    raise RuleError("no version found for dek")
                ciphertext = self._prefix_version(dek.version, ciphertext)
            if field_type == FieldType.STRING:
                return base64.b64encode(ciphertext).decode("utf-8")
            else:
                return self._to_object(field_type, ciphertext)
        elif ctx.rule_mode == RuleMode.READ:
            ciphertext = None
            if field_type == FieldType.STRING:
                ciphertext = base64.b64decode(field_value)
            else:
                ciphertext = self._to_bytes(field_type, field_value)
            if ciphertext is None:
                return field_value

            version = None
            if self._is_dek_rotated():
                version, ciphertext = self._extract_version(ciphertext)
                if version is None:
                    raise RuleError("no version found in ciphertext")
            dek = self._get_or_create_dek(ctx, version)
            key_material_bytes = dek.get_key_material_bytes()
            if key_material_bytes is None:
                raise RuleError("no key material bytes found for dek")
            plaintext = self._cryptor.decrypt(key_material_bytes, ciphertext, Cryptor.EMPTY_AAD)
            return self._to_object(field_type, plaintext)
        else:
            raise RuleError(f"unsupported rule mode {ctx.rule_mode}")

    def _prefix_version(self, version: int, ciphertext: bytes) -> bytes:
        return bytes([_MAGIC_BYTE_V0]) + version.to_bytes(4, byteorder="big") + ciphertext

    def _extract_version(self, ciphertext: bytes) -> Tuple[Optional[int], bytes]:
        if len(ciphertext) < 5:
            return None, ciphertext
        version = int.from_bytes(ciphertext[1:5], byteorder="big")
        return version, ciphertext[5:]

    def _to_bytes(self, field_type: FieldType, value: Any) -> Optional[bytes]:
        if field_type == FieldType.STRING:
            return value.encode("utf-8")
        elif field_type == FieldType.BYTES:
            if isinstance(value, io.BytesIO):
                return value.read()
            return value
        return None

    def _to_object(self, field_type: FieldType, value: bytes) -> Any:
        if field_type == FieldType.STRING:
            return value.decode("utf-8")
        elif field_type == FieldType.BYTES:
            return value
        return None


class AeadWrapper(aead.Aead):
    def __init__(self, config: dict, kek: Kek):
        self._config = config
        self._kek = kek
        self._kms_key_ids = self._get_kms_key_ids()

    def encrypt(self, plaintext: bytes, associated_data: bytes) -> bytes:
        for index, kms_key_id in enumerate(self._kms_key_ids):
            try:
                if self._kek.kms_type is None:
                    raise RuleError("no kms type found for kek")
                aead = self._get_aead(self._config, self._kek.kms_type, kms_key_id)
                return aead.encrypt(plaintext, associated_data)
            except Exception as e:
                log.warning("failed to encrypt with kek %s and kms key id %s", self._kek.name, kms_key_id)
                if index == len(self._kms_key_ids) - 1:
                    raise RuleError(f"failed to encrypt with all KEKs for {self._kek.name}") from e
        raise RuleError("No KEK found for encryption")

    def decrypt(self, ciphertext: bytes, associated_data: bytes) -> bytes:
        for index, kms_key_id in enumerate(self._kms_key_ids):
            try:
                if self._kek.kms_type is None:
                    raise RuleError("no kms type found for kek")
                aead = self._get_aead(self._config, self._kek.kms_type, kms_key_id)
                return aead.decrypt(ciphertext, associated_data)
            except Exception as e:
                log.warning("failed to decrypt with kek %s and kms key id %s", self._kek.name, kms_key_id)
                if index == len(self._kms_key_ids) - 1:
                    raise RuleError(f"failed to decrypt with all KEKs for {self._kek.name}") from e
        raise RuleError("No KEK found for decryption")

    def _get_kms_key_ids(self) -> List[str]:
        kms_key_ids = [self._kek.kms_key_id]
        alternate_kms_key_ids = None
        if self._kek.kms_props is not None:
            alternate_kms_key_ids = self._kek.kms_props.properties.get(ENCRYPT_ALTERNATE_KMS_KEY_IDS)
        if alternate_kms_key_ids is None:
            alternate_kms_key_ids = self._config.get(ENCRYPT_ALTERNATE_KMS_KEY_IDS)
        if alternate_kms_key_ids is not None:
            # Split the comma-separated list of alternate KMS key IDs and append to kms_key_ids
            kms_key_ids.extend([id.strip() for id in alternate_kms_key_ids.split(',') if id.strip()])
        return kms_key_ids  # type: ignore[return-value]

    def _get_aead(self, config: dict, kms_type: str, kms_key_id: str) -> aead.Aead:
        kek_url = kms_type + "://" + kms_key_id
        kms_client = self._get_kms_client(config, kek_url)
        return kms_client.get_aead(kek_url)

    def _get_kms_client(self, config: dict, kek_url: str) -> KmsClient:
        driver = get_kms_driver(kek_url)
        try:
            client = kms_client_from_uri(kek_url)
        except TinkError:
            client = self._register_kms_client(driver, config, kek_url)
        return client

    def _register_kms_client(self, kms_driver: KmsDriver, config: dict, kek_url: str) -> KmsClient:
        kms_client = kms_driver.new_kms_client(config, kek_url)
        register_kms_client(kms_client)
        return kms_client


class FieldEncryptionExecutor(FieldRuleExecutor):

    def __init__(self, clock: Clock = Clock()):
        self.executor = EncryptionExecutor(clock)

    def configure(self, client_conf: dict, rule_conf: dict):
        self.executor.configure(client_conf, rule_conf)

    def type(self) -> str:
        return "ENCRYPT"

    def new_transform(self, ctx: RuleContext) -> FieldTransform:
        executor_transform = self.executor.new_transform(ctx)
        transform = FieldEncryptionExecutorTransform(executor_transform)
        return transform.transform

    def close(self):
        if self.client is not None:
            self.client.__exit__()

    @classmethod
    def register(cls):
        RuleRegistry.register_rule_executor(FieldEncryptionExecutor())

    @classmethod
    def register_with_clock(cls, clock: Clock) -> 'FieldEncryptionExecutor':
        executor = FieldEncryptionExecutor(clock)
        RuleRegistry.register_rule_executor(executor)
        return executor


class FieldEncryptionExecutorTransform(object):

    def __init__(self, executor_transform: 'EncryptionExecutorTransform'):
        self.executor_transform = executor_transform

    def transform(self, ctx: RuleContext, field_ctx: FieldContext, field_value: Any) -> Any:
        return self.executor_transform.transform(ctx, field_ctx.field_type, field_value)
