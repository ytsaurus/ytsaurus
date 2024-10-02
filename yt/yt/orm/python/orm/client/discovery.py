from yt.orm.library.common import (
    InstanceDiscoveryError,
    InvalidDiscoveryResponse,
)

try:
    from yt.packages.six import itervalues
except ImportError:
    from six import itervalues

import functools
import random
import time
import weakref


class MasterDiscovery(object):
    def __init__(self, client, expiration_time, logger, human_readable_orm_name, use_ip6_addresses):
        self._client = weakref.ref(client)
        self._expiration_time = expiration_time
        self._logger = logger
        self._orm_name = human_readable_orm_name
        self._use_ip6_addresses = use_ip6_addresses
        self._clear()

    def _validate_instance_discovery_infos(self, instance_discovery_infos):
        required_fields = ("instance_tag", "alive", "fqdn")
        for required_field in required_fields:
            if not all(required_field in info for info in instance_discovery_infos):
                raise InvalidDiscoveryResponse(
                    'Missing field "{}" in the {} master instance discovery info "{}"'.format(
                        required_field, self._orm_name, instance_discovery_infos
                    )
                )
        if len(set(info["instance_tag"] for info in instance_discovery_infos)) != len(
            instance_discovery_infos
        ):
            self._logger.warning(
                'Encountered duplicate {} master instance tags in discovery info "{}"'.format(
                    self._orm_name, instance_discovery_infos
                )
            )

    def _clear(self):
        self._last_discovery_time = None
        self._tag_to_info = None

    def _validate_initialized(self):
        assert self._last_discovery_time is not None
        assert self._tag_to_info is not None

    def _discover(self):
        client = self._client()
        assert client is not None
        # NB! Method get_masters is called without retries to avoid nested retries.
        discovery_response = client.get_masters(_allow_retries=False)
        if "master_infos" not in discovery_response:
            raise InvalidDiscoveryResponse(
                'Received {} master discovery response with missing "master_infos" field "{}"'.format(
                    self._orm_name, discovery_response
                )
            )
        instance_discovery_infos = discovery_response["master_infos"]
        self._validate_instance_discovery_infos(instance_discovery_infos)
        instance_discovery_infos = list(
            filter(lambda info: info["alive"], instance_discovery_infos)
        )
        self._tag_to_info = dict((info["instance_tag"], info) for info in instance_discovery_infos)

    def _is_refresh_required(self):
        if self._last_discovery_time is None:
            self._logger.debug("Discovery initialization required")
            return True
        if time.time() - self._last_discovery_time >= self._expiration_time / 1000.0:
            self._logger.debug(
                "Discovery refresh required since at least %s second(s) passed since last discovery",
                self._expiration_time / 1000.0,
            )
            return True
        if not self._tag_to_info:
            # Helpful after masters restart, especially in tests.
            self._logger.debug(
                "Discovery refresh required since there is no alive instance in the last discovery info"
            )
            return True
        return False

    def _refresh(self):
        if self._is_refresh_required():
            self._clear()
            self._discover()
            self._logger.debug(
                "Discovered %d alive %s master instances: [%s]",
                len(self._tag_to_info),
                self._orm_name,
                ", ".join(info["fqdn"] for info in itervalues(self._tag_to_info)),
            )
            self._last_discovery_time = time.time()

    def _get_address_field_name(self, transport):
        return "{}{}_address".format(transport, "_ip6" if self._use_ip6_addresses else "")

    def uses_ip6_addresses(self):
        return self._use_ip6_addresses

    def get_random_instance_address(self, transport):
        self._refresh()
        self._validate_initialized()
        instance_discovery_infos = list(itervalues(self._tag_to_info))
        instance_transport_filter = functools.partial(self.instance_supports_transport, transport)
        instance_discovery_infos = list(filter(instance_transport_filter, instance_discovery_infos))
        if len(instance_discovery_infos) == 0:
            raise InstanceDiscoveryError(
                'Could not find any alive {} master instance with transport "{}" support'.format(
                    self._orm_name, transport
                )
            )
        return self.get_instance_address(
            transport,
            random.choice(instance_discovery_infos),
        )

    def get_instance_address_by_tag(self, tag, transport):
        self._refresh()
        self._validate_initialized()
        if tag not in self._tag_to_info:
            raise InstanceDiscoveryError(
                "Could not find alive {} master with instance_tag {}".format(self._orm_name, tag)
            )
        return self.get_instance_address(transport, self._tag_to_info[tag])

    def instance_supports_transport(self, transport, instance_discovery_info):
        return self._get_address_field_name(transport) in instance_discovery_info

    def get_instance_address(self, transport, instance_discovery_info):
        field_name = self._get_address_field_name(transport)
        if field_name not in instance_discovery_info:
            raise InstanceDiscoveryError(
                'Master instance {} does not support transport "{}"'.format(
                    instance_discovery_info["fqdn"], transport
                )
            )
        return str(instance_discovery_info[field_name])
