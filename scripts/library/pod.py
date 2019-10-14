# -*- coding: utf-8 -*-

from six.moves import reduce


def combine_pods(pod_set_id, pods, combine_labels_callback=None):
    def impl(p1, p2):
        meta = dict(type="pod", pod_set_id=pod_set_id)
        spec = dict(resource_requests=dict(), ip6_address_requests=[], disk_volume_requests=[])

        primary_resource_fields = ("vcpu_guarantee", "vcpu_limit", "memory_guarantee", "memory_limit")
        for field in primary_resource_fields:
            spec["resource_requests"][field] = \
                p1["spec"]["resource_requests"][field] + \
                p2["spec"]["resource_requests"][field]

        disk_storage_classes = ("hdd", "ssd")
        for disk_storage_class in disk_storage_classes:
            quota_policy_capacity = 0
            for pod in (p1, p2):
                for disk_volume_request in pod["spec"]["disk_volume_requests"]:
                    if disk_volume_request["storage_class"] == disk_storage_class:
                        quota_policy_capacity += disk_volume_request["quota_policy"]["capacity"]
            spec["disk_volume_requests"].append(
                dict(
                    id=disk_storage_class,
                    storage_class=disk_storage_class,
                    quota_policy=dict(capacity=quota_policy_capacity),
                )
            )

        pod = dict(meta=meta, spec=spec)
        if combine_labels_callback is not None:
            pod["labels"] = combine_labels_callback(p1.get("labels", {}), p2.get("labels", {}))

        return pod

    return reduce(impl, pods)


def generate_pod_set(id_, max_pod_per_node=None):
    pod_set = dict(meta=dict(type="pod_set", id=id_))
    if max_pod_per_node is not None:
        pod_set["spec"] = dict()
        pod_set["spec"]["antiaffinity_constraints"] = [
            dict(key="node", max_pods=max_pod_per_node)
        ]
    return pod_set


def _generate_disk_volume_request(storage_class, capacity):
    return dict(
        id=storage_class,
        storage_class=storage_class,
        quota_policy=dict(capacity=capacity),
    )


def generate_pod(pod_set_id,
                 cpu_guarantee,
                 cpu_limit,
                 memory_guarantee,
                 memory_limit,
                 hdd_capacity=0,
                 ssd_capacity=0,
                 labels=None):
    disk_volume_requests = []
    if hdd_capacity > 0:
        disk_volume_requests.append(_generate_disk_volume_request("hdd", hdd_capacity))
    if ssd_capacity > 0:
        disk_volume_requests.append(_generate_disk_volume_request("ssd", ssd_capacity))
    meta = dict(type="pod", pod_set_id=pod_set_id)
    spec = dict(
        resource_requests=dict(
            vcpu_guarantee=cpu_guarantee,
            vcpu_limit=cpu_limit,
            memory_guarantee=memory_guarantee,
            memory_limit=memory_limit,
        ),
        ip6_address_requests=[
        ],
        disk_volume_requests=disk_volume_requests,
    )
    pod = dict(meta=meta, spec=spec)
    if labels is not None:
        pod["labels"] = labels
    return pod


def get_disk_request_capacity(disk_request):
    if "quota_policy" in disk_request:
        return disk_request["quota_policy"]["capacity"]
    else:
        return disk_request["exclusive_policy"]["min_capacity"]
