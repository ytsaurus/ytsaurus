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

import abc
from typing import List

from tink import KmsClient

from confluent_kafka.schema_registry.serde import RuleError


class KmsDriver(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def get_key_url_prefix(self) -> str:
        raise NotImplementedError()

    @abc.abstractmethod
    def new_kms_client(self, conf: dict, key_url: str) -> KmsClient:
        raise NotImplementedError()


_kms_drivers: List[KmsDriver] = []


# Adds driver to a global list of KmsDrivers.
def register_kms_driver(driver: KmsDriver) -> None:
    """Adds a KMS driver to a global list.

    Args:
        driver: KmsDriver to be registered
    """
    _kms_drivers.append(driver)


def get_kms_driver(key_url: str) -> KmsDriver:
    """Returns the first KMS client that supports key_url."""
    for driver in _kms_drivers:
        if key_url.startswith(driver.get_key_url_prefix()):
            return driver
    raise RuleError('no KMS driver found for key URL: ' + key_url)


def reset_kms_drivers() -> None:
    """Removes all registered clients. Internal and only used for tests."""
    _kms_drivers.clear()
