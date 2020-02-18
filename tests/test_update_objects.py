from .conftest import DEFAULT_ACCOUNT_ID, create_pod_set

from yp.local import set_account_infinite_resource_limits

from yt.wrapper.ypath import ypath_join

from yt.common import update

from yt.packages.six.moves import xrange

import collections
import copy
import pytest
import random


def set_by_path(value, path, subvalue):
    def get_index(value, token):
        if isinstance(value, list):
            return int(token)
        return token

    assert path.startswith("/")
    assert not path.endswith("/")
    path = path[1:]

    tokens = path.split("/")
    assert len(tokens) > 0

    for token in tokens[:-1]:
        value = value[get_index(value, token)]

    value[get_index(value, tokens[-1])] = subvalue


def validate_subequality(value, subvalue):
    if isinstance(value, list):
        assert value == subvalue
    else:
        assert update(value, subvalue) == value


class IntegerValue(object):
    def __init__(self):
        self._value = 0

    def get(self):
        return self._value

    def advance(self):
        self._value += 1


class CyclicValue(object):
    def __init__(self, values):
        self._values = values
        self._position = 0

    def get(self):
        return self._values[self._position]

    def advance(self):
        self._position = (self._position + 1) % len(self._values)


def get_mapped_value_class(value_class, mapper):
    class MappedValue(object):
        def __init__(self):
            self._value = value_class()

        def get(self):
            return mapper(self._value.get())

        def advance(self):
            self._value.advance()
    return MappedValue


StringValue = get_mapped_value_class(IntegerValue, str)
PermissionValue = get_mapped_value_class(lambda: CyclicValue(["read", "write"]), lambda x: x)

AttributeBase = collections.namedtuple(
    "AttributeBase",
    [
        "base_path",
        "initial_value",
        "variable_subpath",
        "variable_value",
    ],
)


class Attribute(AttributeBase):
    def get_variable_path(self):
        return ypath_join(self.base_path, self.variable_subpath)

    def get_value(self):
        value = copy.deepcopy(self.initial_value)
        set_by_path(value, self.variable_subpath, self.variable_value.get())
        return value


class YPObject(object):
    def __init__(self, object_type, object_id, attributes):
        self._object_type = object_type
        self._object_id = object_id
        self._attributes = attributes

    def initialize(self, yp_client):
        updates = [
            dict(
                path=attribute.base_path,
                value=attribute.initial_value,
            )
            for attribute in self._attributes
        ]
        yp_client.update_object(
            object_type=self._object_type,
            object_id=self._object_id,
            set_updates=updates,
        )

    def get_max_update_count(self):
        return len(self._attributes)

    def generate_random_updates(self, update_count):
        assert 0 < update_count <= self.get_max_update_count()
        attributes = random.sample(self._attributes, update_count)
        updates = []
        for attribute in attributes:
            attribute.variable_value.advance()
            updates.append(dict(
                path=attribute.get_variable_path(),
                value=attribute.variable_value.get(),
            ))
        return [
            dict(
                object_type=self._object_type,
                object_id=self._object_id,
                set_updates=updates,
            )
        ]

    def validate(self, yp_client):
        selectors = []
        for attribute in self._attributes:
            selectors.append(attribute.base_path)
        values = yp_client.get_object(
            self._object_type,
            self._object_id,
            selectors=selectors,
        )
        assert len(values) == len(self._attributes)
        for attribute, actual_value in zip(self._attributes, values):
            validate_subequality(actual_value, attribute.get_value())


def get_common_attributes():
    return [
        Attribute(
            base_path="/labels/some_map",
            initial_value=dict(fixed_value="value", variable_value="0"),
            variable_subpath="/variable_value",
            variable_value=StringValue(),
        ),
        Attribute(
            base_path="/meta/acl",
            initial_value=[dict(permissions=["read"], subjects=["root"], action="allow")],
            variable_subpath="/0/permissions/0",
            variable_value=PermissionValue(),
        ),
    ]


def create_pod_yp_object(yp_client, pod_set_id):
    pod_id = yp_client.create_object("pod", attributes=dict(meta=dict(pod_set_id=pod_set_id)))
    attributes = [
        Attribute(
            base_path="/spec/secrets",
            initial_value=dict(some_secret=dict(secret_id="id", secret_version="0", delegation_token="token")),
            variable_subpath="/some_secret/secret_version",
            variable_value=StringValue(),
        ),
        Attribute(
            base_path="/spec/dynamic_attributes",
            initial_value=dict(annotations=["0"]),
            variable_subpath="/annotations/0",
            variable_value=StringValue(),
        ),
        Attribute(
            base_path="/spec/disk_volume_requests",
            initial_value=[dict(id="id1", storage_class="0", quota_policy=dict(capacity=0))],
            variable_subpath="/0/storage_class",
            variable_value=StringValue(),
        ),
    ]
    attributes.extend(get_common_attributes())
    return YPObject("pod", pod_id, attributes)


def create_node_yp_object(yp_client):
    node_id = yp_client.create_object("node")
    attributes = [
        Attribute(
            base_path="/spec",
            initial_value=dict(short_name="shortname", network_module_id="0"),
            variable_subpath="/network_module_id",
            variable_value=StringValue(),
        )
    ]
    attributes.extend(get_common_attributes())
    return YPObject("node", node_id, attributes)


def create_resource_yp_object(yp_client):
    node_id = yp_client.create_object("node")
    resource_id = yp_client.create_object(
        "resource",
        attributes=dict(
            meta=dict(node_id=node_id),
            spec=dict(cpu=dict(total_capacity=0)),
        ),
    )
    attributes = [
        Attribute(
            base_path="/spec",
            initial_value=dict(cpu=dict(total_capacity=0, cpu_to_vcpu_factor=10.0)),
            variable_subpath="/cpu/total_capacity",
            variable_value=IntegerValue(),
        ),
    ]
    attributes.extend(get_common_attributes())
    return YPObject("resource", resource_id, attributes)


@pytest.mark.usefixtures("yp_env")
class TestUpdateObjects(object):
    # Also hope to test internal server lookups.
    def test_partial_scalar_proto_fields_update(self, yp_env):
        with yp_env.yp_instance.create_client(config=dict(user="root")) as yp_client:
            set_account_infinite_resource_limits(yp_client, DEFAULT_ACCOUNT_ID)

            objects = []

            object_per_type = 10
            pod_set_id = yp_client.create_object("pod_set")
            for _ in xrange(object_per_type):
                objects.append(create_pod_yp_object(yp_client, pod_set_id))
                objects.append(create_resource_yp_object(yp_client))
                objects.append(create_node_yp_object(yp_client))

            for obj in objects:
                obj.initialize(yp_client)

            iteration_count = 10
            object_per_iteration = 10
            for _ in xrange(iteration_count):
                object_sample = random.sample(objects, object_per_iteration)

                set_updates = []
                for obj in object_sample:
                    update_count = random.randint(1, obj.get_max_update_count())
                    set_updates.extend(obj.generate_random_updates(update_count))

                yp_client.update_objects(set_updates)

                for obj in object_sample:
                    obj.validate(yp_client)

    def test_batch_add_to_array_end(self, yp_env):
        username = "simple_user"
        yp_env.yp_client.create_object("user", attributes={"meta": {"id": username}})
        yp_env.sync_access_control()

        ace_types = ["read", "write", "use"]

        with yp_env.yp_instance.create_client(config=dict(user=username)) as yp_client:
            pod_set_ids = [create_pod_set(yp_client) for _ in range(2)]

            start_acls = yp_client.get_objects("pod_set", pod_set_ids, ["/meta/acl"])

            yp_client.update_objects([dict(
                object_type="pod_set",
                object_id=pod_set_ids[0],
                set_updates=[dict(
                    path="/meta/acl/end",
                    value=dict(action="allow", subjects=[username], permissions=[permission]))],
            ) for permission in ace_types])

            yp_client.update_object("pod_set", pod_set_ids[1], [dict(
                path="/meta/acl/end",
                value=dict(action="allow", subjects=[username], permissions=[permission])
            ) for permission in ace_types])

            end_acls = yp_client.get_objects("pod_set", pod_set_ids, ["/meta/acl"])

            for acls in [start_acls, end_acls]:
                assert acls[0] == acls[1]

            for start_acl, end_acl in zip(start_acls, end_acls):
                assert len(end_acl[0]) == len(start_acl[0]) + len(ace_types)
                assert all(map(lambda pair: pair[0] == pair[1], zip(start_acl[0], end_acl[0])))

                for ace_type, ace in zip(ace_types, end_acl[0][len(start_acl):]):
                    assert ace["action"] == "allow"
                    assert ace["subjects"] == [username]
                    assert ace["permissions"] == [ace_type]
