from yp.scripts.library.cluster_snapshot import SchedulerCluster

import copy
import logging
import uuid


def generate_uuid():
    return uuid.uuid4().hex[:16]


class SchedulerObjectsHistory(SchedulerCluster):
    def __len__(self):
        result = 0
        for field_name in self._fields:
            result += len(getattr(self, field_name))
        return result

    def drop_pod_to_node_assignment(self):
        logging.info("Dropping pod to node assignment")
        for pod in self.pods:
            spec = pod.get("spec", {})
            if "node_id" in spec:
                spec["node_id"] = ""

    def make_nodes_schedulable(self):
        logging.info("Making nodes schedulable")
        for attributes in self.nodes:
            attributes["control"] = dict(update_hfsm_state=dict(state="up", message=""))

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
        for field_name in self._fields:
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

    def select_object_attributes(self, object_type_to_attributes):
        # Only leave object attributes present in `object_type_to_attributes` map.
        # E.g. object_type_to_attributes = {
        #    "node": ["/meta/id", "/spec"],
        #    "pod": ...
        #    ...
        # }
        objects = []
        for type_, attrs in object_type_to_attributes.iteritems():
            filtered_objects = []
            for obj in self.get_by_type(type_):
                filtered_objects.append(extract_attributes(obj, attrs))
            objects.extend(filtered_objects)
        return self.__class__.from_flatten(objects)

    def drop_none_attributes(self):
        def drop_nones(obj):
            del_list = []
            for key, value in obj.iteritems():
                if value is None:
                    del_list.append(key)
                elif isinstance(value, dict):
                    drop_nones(value)
            for key in del_list:
                del obj[key]

        for field_name in self._fields:
            for obj in getattr(self, field_name, []):
                drop_nones(obj)

    def drop_pods_with_nonexistent_network_project(self, limit):
        logging.info("Dropping pods with non-existent network projects")
        project_ids = set(proj["meta"]["id"] for proj in self.network_projects)

        def check_pod(pod):
            requests = pod["spec"].get("ip6_address_requests", []) + pod["spec"].get(
                "ip6_subnet_requests", []
            )
            missing_projects = set(req["network_id"] for req in requests) - project_ids
            if missing_projects:
                logging.warning(
                    "Dropping pod '%s' due to nonexistent network project(s): '%s'",
                    pod["meta"]["id"],
                    "', '".join(missing_projects),
                )
            return not missing_projects

        pods = [pod for pod in self.pods if check_pod(pod)]
        lost = len(self.pods) - len(pods)
        if lost > limit:
            raise RuntimeError("Dropped pods limit exceeded: {}".format(lost))

        return self.replace(pods=pods)

    def drop_internet_address_requests(self):
        logging.info("Dropping pods internet address requests")
        for pod in self.pods:
            requests = [
                req
                for req in pod["spec"].get("ip6_address_requests", [])
                if not req.get("enable_internet")
            ]
            pod["spec"]["ip6_address_requests"] = requests

    def drop_pods_on_nonexistent_nodes(self, limit):
        logging.info("Dropping pods on non-existent nodes")
        node_ids = [node["meta"]["id"] for node in self.nodes]

        def check_pod(pod):
            node_id = pod["spec"].get("node_id")
            if node_id and node_id not in node_ids:
                logging.warning(
                    "Dropping pod '%s' due to non-existent node '%s'", pod["meta"]["id"], node_id
                )
                return False
            return True

        pods = [pod for pod in self.pods if check_pod(pod)]

        lost = len(self.pods) - len(pods)
        if lost > limit:
            raise RuntimeError("Dropped pods limit exceeded: {}".format(lost))

        return self.replace(pods=pods)

    def drop_non_up_nodes(self):
        logging.info("Dropping non-up nodes")

        remained_nodes = []
        for node in self.nodes:
            if node["status"].get("hfsm", {}).get("state", "unknown") == "up":
                remained_nodes.append(node)

        dropped_node_count = len(self.nodes) - len(remained_nodes)
        if dropped_node_count <= 0:
            logging.info("No non-up nodes found")
            return
        logging.warn("Dropping %d non-up nodes", dropped_node_count)

        node_ids = set()
        for node in remained_nodes:
            node_ids.add(node["meta"]["id"])

        remained_resources = []
        for resource in self.resources:
            if resource["meta"]["node_id"] in node_ids:
                remained_resources.append(resource)

        dropped_resource_count = len(self.resources) - len(remained_resources)
        if dropped_resource_count > 0:
            logging.warn("Dropping %d resources of non-up nodes", dropped_resource_count)

        remained_pods = []
        for pod in self.pods:
            node_id = pod["spec"].get("node_id")
            if not node_id or node_id in node_ids:
                remained_pods.append(pod)

        dropped_pod_count = len(self.pods) - len(remained_pods)
        if dropped_pod_count > 0:
            logging.warn("Dropping %d pods of non-up nodes", dropped_pod_count)

        return self.replace(nodes=remained_nodes, resources=remained_resources, pods=remained_pods)


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
