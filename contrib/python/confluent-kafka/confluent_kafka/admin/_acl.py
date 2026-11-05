# Copyright 2022 Confluent Inc.
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

import functools
from enum import Enum
from typing import Any, Dict, List, Tuple, Union

from .. import cimpl as _cimpl
from .._util import ConversionUtil, ValidationUtil
from ._resource import ResourcePatternType, ResourceType

try:
    string_type = basestring  # type: ignore[name-defined]
except NameError:
    string_type = str


class AclOperation(Enum):
    """
    Enumerates the different types of ACL operation.
    """

    UNKNOWN = _cimpl.ACL_OPERATION_UNKNOWN  #: Unknown
    ANY = _cimpl.ACL_OPERATION_ANY  #: In a filter, matches any AclOperation
    ALL = _cimpl.ACL_OPERATION_ALL  #: ALL the operations
    READ = _cimpl.ACL_OPERATION_READ  #: READ operation
    WRITE = _cimpl.ACL_OPERATION_WRITE  #: WRITE operation
    CREATE = _cimpl.ACL_OPERATION_CREATE  #: CREATE operation
    DELETE = _cimpl.ACL_OPERATION_DELETE  #: DELETE operation
    ALTER = _cimpl.ACL_OPERATION_ALTER  #: ALTER operation
    DESCRIBE = _cimpl.ACL_OPERATION_DESCRIBE  #: DESCRIBE operation
    CLUSTER_ACTION = _cimpl.ACL_OPERATION_CLUSTER_ACTION  #: CLUSTER_ACTION operation
    DESCRIBE_CONFIGS = _cimpl.ACL_OPERATION_DESCRIBE_CONFIGS  #: DESCRIBE_CONFIGS operation
    ALTER_CONFIGS = _cimpl.ACL_OPERATION_ALTER_CONFIGS  #: ALTER_CONFIGS operation
    IDEMPOTENT_WRITE = _cimpl.ACL_OPERATION_IDEMPOTENT_WRITE  #: IDEMPOTENT_WRITE operation

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, AclOperation):
            return NotImplemented
        return self.value < other.value


class AclPermissionType(Enum):
    """
    Enumerates the different types of ACL permission types.
    """

    UNKNOWN = _cimpl.ACL_PERMISSION_TYPE_UNKNOWN  #: Unknown
    ANY = _cimpl.ACL_PERMISSION_TYPE_ANY  #: In a filter, matches any AclPermissionType
    DENY = _cimpl.ACL_PERMISSION_TYPE_DENY  #: Disallows access
    ALLOW = _cimpl.ACL_PERMISSION_TYPE_ALLOW  #: Grants access

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, AclPermissionType):
            return NotImplemented
        return self.value < other.value


@functools.total_ordering
class AclBinding(object):
    """
    Represents an ACL binding that specify the operation and permission type for a specific principal
    over one or more resources of the same type. Used by :meth:`AdminClient.create_acls`,
    returned by :meth:`AdminClient.describe_acls` and :meth:`AdminClient.delete_acls`.

    Parameters
    ----------
    restype : ResourceType
        The resource type.
    name : str
        The resource name, which depends on the resource type. For :attr:`ResourceType.BROKER`,
        the resource name is the broker id.
    resource_pattern_type : ResourcePatternType
        The resource pattern, relative to the name.
    principal : str
        The principal this AclBinding refers to.
    host : str
        The host that the call is allowed to come from.
    operation: AclOperation
        The operation/s specified by this binding.
    permission_type: AclPermissionType
        The permission type for the specified operation.
    """

    def __init__(
        self,
        restype: Union[ResourceType, str, int],
        name: str,
        resource_pattern_type: Union[ResourcePatternType, str, int],
        principal: str,
        host: str,
        operation: Union[AclOperation, str, int],
        permission_type: Union[AclPermissionType, str, int],
    ) -> None:
        self.restype = restype
        self.name = name
        self.resource_pattern_type = resource_pattern_type
        self.principal = principal
        self.host = host
        self.operation = operation
        self.permission_type = permission_type
        self._convert_args()
        # for the C code
        self.restype_int = int(self.restype.value)  # type: ignore[union-attr]
        self.resource_pattern_type_int = int(self.resource_pattern_type.value)  # type: ignore[union-attr]
        self.operation_int = int(self.operation.value)  # type: ignore[union-attr]
        self.permission_type_int = int(self.permission_type.value)  # type: ignore[union-attr]

    def _convert_enums(self) -> None:
        self.restype = ConversionUtil.convert_to_enum(self.restype, ResourceType)  # type: ignore[assignment]
        self.resource_pattern_type = ConversionUtil.convert_to_enum(
            self.resource_pattern_type, ResourcePatternType
        )  # type: ignore[assignment]
        self.operation = ConversionUtil.convert_to_enum(self.operation, AclOperation)  # type: ignore[assignment]
        self.permission_type = ConversionUtil.convert_to_enum(
            self.permission_type, AclPermissionType
        )  # type: ignore[assignment]

    def _check_forbidden_enums(self, forbidden_enums: Dict[str, List[Enum]]) -> None:
        for k, v in forbidden_enums.items():
            enum_value = getattr(self, k)
            if enum_value in v:
                raise ValueError("Cannot use enum %s, value %s in this class" % (k, enum_value.name))

    def _not_none_args(self) -> List[str]:
        return ["restype", "name", "resource_pattern_type", "principal", "host", "operation", "permission_type"]

    def _string_args(self) -> List[str]:
        return ["name", "principal", "host"]

    def _forbidden_enums(self) -> Dict[str, List[Enum]]:
        return {
            "restype": [ResourceType.ANY],
            "resource_pattern_type": [ResourcePatternType.ANY, ResourcePatternType.MATCH],
            "operation": [AclOperation.ANY],
            "permission_type": [AclPermissionType.ANY],
        }

    def _convert_args(self) -> None:
        not_none_args = self._not_none_args()
        string_args = self._string_args()
        forbidden_enums = self._forbidden_enums()
        ValidationUtil.check_multiple_not_none(self, not_none_args)
        ValidationUtil.check_multiple_is_string(self, string_args)
        self._convert_enums()
        self._check_forbidden_enums(forbidden_enums)

    def __repr__(self) -> str:
        type_name = type(self).__name__
        return "%s(%s,%s,%s,%s,%s,%s,%s)" % ((type_name,) + self._to_tuple())

    def _to_tuple(self) -> Tuple[ResourceType, str, ResourcePatternType, str, str, AclOperation, AclPermissionType]:
        return (
            self.restype,
            self.name,
            self.resource_pattern_type,  # type: ignore[return-value]
            self.principal,
            self.host,
            self.operation,
            self.permission_type,
        )

    def __hash__(self) -> int:
        return hash(self._to_tuple())

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, AclBinding):
            return NotImplemented
        return self._to_tuple() < other._to_tuple()

    def __eq__(self, other: object) -> Any:
        if not isinstance(other, AclBinding):
            return NotImplemented
        return self._to_tuple() == other._to_tuple()


class AclBindingFilter(AclBinding):
    """
    Represents an ACL binding filter used to return a list of ACL bindings matching some or all of its attributes.
    Used by :meth:`AdminClient.describe_acls` and :meth:`AdminClient.delete_acls`.

    Parameters
    ----------
    restype : ResourceType
        The resource type, or :attr:`ResourceType.ANY` to match any value.
    name : str
        The resource name to match.
        None matches any value.
    resource_pattern_type : ResourcePatternType
        The resource pattern, :attr:`ResourcePatternType.ANY` to match any value or
        :attr:`ResourcePatternType.MATCH` to perform pattern matching.
    principal : str
        The principal to match, or None to match any value.
    host : str
        The host to match, or None to match any value.
    operation: AclOperation
        The operation to match or :attr:`AclOperation.ANY` to match any value.
    permission_type: AclPermissionType
        The permission type to match or :attr:`AclPermissionType.ANY` to match any value.
    """

    def _not_none_args(self) -> List[str]:
        return ["restype", "resource_pattern_type", "operation", "permission_type"]

    def _forbidden_enums(self) -> Dict[str, List[Enum]]:
        return {
            "restype": [ResourceType.UNKNOWN],
            "resource_pattern_type": [ResourcePatternType.UNKNOWN],
            "operation": [AclOperation.UNKNOWN],
            "permission_type": [AclPermissionType.UNKNOWN],
        }
