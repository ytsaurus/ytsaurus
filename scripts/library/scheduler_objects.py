# -*- coding: utf-8 -*-

from yt import yson

import collections
import copy
import json
import logging
import uuid


def generate_uuid():
    return uuid.uuid4().hex[:16]


YpSchedulerObjectsBase = collections.namedtuple(
    "YpSchedulerObjectsBase",
    [
        "pods",
        "pod_sets",
        "resources",
        "nodes"
    ]
)

# YpSchedulerObjectsBase fields are supposed to have name of the form (#object_type)s.
assert all(field_name.endswith("s") for field_name in YpSchedulerObjectsBase._fields)


class YpSchedulerObjects(YpSchedulerObjectsBase):
    def __len__(self):
        result = 0
        for field_name in self._fields:
            result += len(getattr(self, field_name))
        return result

    def make_schedulable(self):
        logging.info("Making pods schedulable")
        for attributes in self.pods:
            attributes["spec"]["enable_scheduling"] = True

        logging.info("Making nodes schedulable")
        for attributes in self.nodes:
            attributes["control"] = dict(update_hfsm_state=dict(
                state="up",
                message=""
            ))

    def set_pod_sets_node_segment(self, node_segment_id):
        logging.info("Setting node segment for pod sets")
        for attributes in self.pod_sets:
            if "spec" not in attributes:
                attributes["spec"] = {}
            attributes["spec"]["node_segment_id"] = node_segment_id

    def set_pod_sets_account_id(self, account_id):
        logging.info("Setting account for pod sets")
        for attributes in self.pod_sets:
            if "spec" not in attributes:
                attributes["spec"] = {}
            attributes["spec"]["account_id"] = account_id

    # YP master does not allow to create object with present /meta/type field.
    def erase_meta_types(self):
        logging.info("Erasing /meta/type")
        for field_name in YpSchedulerObjects._fields:
            objects = getattr(self, field_name)
            for attributes in objects:
                attributes["meta"].pop("type")

    # Generate long ids at the client-side to overcome collisions.
    def generate_meta_ids(self):
        logging.info("Generating object ids")
        for objects in (self.pods, self.resources):
            if len(objects) == 0 or "id" in objects[0]["meta"]:
                continue
            for object_ in objects:
                object_["meta"]["id"] = generate_uuid()

    def get_by_type(self, type_):
        return getattr(self, type_ + "s")

    def select_object_attributes(self, object_type_to_attributes):
        # Only leave object attributes present in `object_type_to_attributes` map.
        # E.g. object_type_to_attributes = {
        #    "node": ["/meta/id", "/spec"],
        #    "pod": ...
        #    ...
        # }
        logging.info("Selecting object attributes")
        result = {}
        for field_name in self._fields:
            filtered_objects = []
            attrs = object_type_to_attributes[field_name[:-1]]
            for obj in getattr(self, field_name):
                filtered_objects.append(extract_attributes(obj, attrs))
            result[field_name] = filtered_objects
        return YpSchedulerObjects(**result)

    def drop_none_attributes(self):
        def drop_nones(obj):
            del_list = []
            for key, value in obj.iteritems():
                if value is None or isinstance(key, yson.YsonEntity):
                    del_list.append(key)
                elif isinstance(value, dict):
                    drop_nones(value)
            for key in del_list:
                del obj[key]

        for field_name in self._fields:
            for obj in getattr(self, field_name, []):
                drop_nones(obj)


def extract_attributes(obj, attrs):
    new = {}
    for attr in attrs:
        parts = [p for p in attr.split("/") if p]
        old_subobj = obj
        new_subobj = new
        for part in parts[:-1]:
            if part not in new_subobj:
                new_subobj[part] = {}
            new_subobj = new_subobj[part]
            old_subobj = old_subobj[part]
        new_subobj[parts[-1]] = copy.deepcopy(old_subobj.get(parts[-1]))
    return new


def read_yp_scheduler_objects(yp_scheduler_objects_file_path):
    logging.info("Reading YP scheduler objects from file %s", yp_scheduler_objects_file_path)

    data = None
    with open(yp_scheduler_objects_file_path, "rb") as yp_scheduler_objects_file:
        data = json.load(yp_scheduler_objects_file)
    assert data is not None

    def filter_by_type(type_):
        return filter(lambda attributes: attributes["meta"]["type"] == type_, data)

    class_field_name_to_value = dict(
        (field_name, filter_by_type(field_name[:-1]))
        for field_name in YpSchedulerObjects._fields
    )
    yp_scheduler_objects = YpSchedulerObjects(**class_field_name_to_value)

    for field_name in YpSchedulerObjects._fields:
        logging.info(
            "Read %d YP objects of type '%s'",
            len(getattr(yp_scheduler_objects, field_name)),
            field_name,
        )

    return yp_scheduler_objects
