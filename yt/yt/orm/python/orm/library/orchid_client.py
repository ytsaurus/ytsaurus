from .monitoring_client import OrmMonitoringClient

from yt.wrapper.ypath import ypath_join
from yt.wrapper.retries import run_with_retries

import functools
import operator


def get_leader_fqdn(yt_client, service_cypress_path, service_name, throw_if_no_leader=True):
    leader_locks = yt_client.get(ypath_join(service_cypress_path, "leader", "@locks"))

    assert 1 >= len(leader_locks), "Expected at most 1 leader lock, got {}".format(
        len(leader_locks),
    )
    if len(leader_locks) == 0:
        if not throw_if_no_leader:
            return None
        raise Exception("None of {} instances is leading".format(service_name))

    return yt_client.get(ypath_join("#{}".format(leader_locks[0]["transaction_id"]), "@fqdn"))


class OrmOrchidClient(object):
    def __init__(self, yt_client, orm_path, service_name, human_readable_service_name=None):
        self._yt_client = yt_client
        self._service_cypress_path = ypath_join(orm_path, service_name)
        self._instances_path = ypath_join(self._service_cypress_path, "instances")
        self.service_name = service_name
        self.human_readable_service_name = human_readable_service_name or service_name

    def list_instances(self):
        return list(self._yt_client.list(self._instances_path))

    def exists_at_instance(self, instance_fqdn, path):
        orchid_path = ypath_join(self._instances_path, instance_fqdn, "orchid")
        return self._yt_client.exists(ypath_join(orchid_path, path))

    def _get_from_monitoring(self, monitoring_address, path):
        client = OrmMonitoringClient(monitoring_address)

        root_path, _, inner_path = str(path).lstrip("/").partition("/")
        content = client.get("/" + root_path, service="/orchid")

        if not inner_path:
            return content
        return functools.reduce(operator.getitem, inner_path.split('/'), content)

    def get_at_instance(self, instance_fqdn, path, *args, **kwargs):
        instance_path = ypath_join(self._instances_path, instance_fqdn)
        orchid_path = ypath_join(instance_path, "orchid")
        if self._yt_client.exists(orchid_path):
            return self._yt_client.get(ypath_join(orchid_path, path), *args, **kwargs)

        monitoring_address = self._yt_client.get_attribute(instance_path, "monitoring_address", None)
        if not monitoring_address:
            raise Exception("Instance {} of {} has neither orchid nor monitoring configured".format(
                instance_fqdn, self.service_name
            ))
        return self._get_from_monitoring(monitoring_address, path)

    def get_leader_instance(self, throw_if_no_leader=True):
        leader = run_with_retries(
            lambda: get_leader_fqdn(
                self._yt_client, self._service_cypress_path, self.human_readable_service_name, throw_if_no_leader
            ),
            retry_count=4,
            backoff=2.0,
            exceptions=(Exception,),
        )
        return leader

    def get_default_instance(self):
        try:
            return self.get_leader_instance()
        except Exception:
            pass
        instances = self.list_instances()
        assert instances, "Could not find any {} instance".format(self.human_readable_service_name)
        return instances[0]

    def get(self, path, *args, **kwargs):
        return self.get_at_instance(self.get_default_instance(), path, *args, **kwargs)
