from yt.wrapper.errors import YtResolveError

from yt.wrapper.transaction import Transaction

import yt.logger as logger

import time


class BanManager(object):
    def __init__(
        self,
        yt_client,
        orm_path,
    ):
        self._yt_client = yt_client
        self._orm_path = orm_path

    def _get_instances_banned_by_fqdn_path(self):
        return "{}/master/instances_banned_by_fqdn".format(self._orm_path)

    def _get_instances_path(self):
        return "{}/master/instances".format(self._orm_path)

    def ban(self, fqdns, ignore_banned_masters=False):
        with Transaction(client=self._yt_client):
            for fqdn in fqdns:
                instance_banned_by_fqdn_path = "/".join(
                    [self._get_instances_banned_by_fqdn_path(), fqdn]
                )
                self._yt_client.create(
                    "document",
                    instance_banned_by_fqdn_path,
                    ignore_existing=ignore_banned_masters,
                    recursive=True,
                )
        # Ensure that no transaction is locking banned instance nodes after a minute.
        transaction_check_time_s = 60
        logger.info(
            "Waiting for {} seconds for masters to stop locking instance locks".format(
                transaction_check_time_s
            )
        )
        time.sleep(transaction_check_time_s)
        with Transaction(client=self._yt_client):
            try:
                for fqdn in fqdns:
                    instance_transaction_path = "/".join(
                        [self._get_instances_path(), fqdn, "@locks", "0", "transaction_id"]
                    )
                    transaction_id = self._yt_client.get(instance_transaction_path)
                    raise RuntimeError(
                        "Instance {} is still locking Cypress node {} with transaction {}. You can try banning it manually".format(
                            fqdn, "/".join([self._get_instances_path(), fqdn]), transaction_id
                        )
                    )
            except YtResolveError:
                pass
        # Wait for masters to get their instance transaction aborted.
        # The delay consists of ban checker period - not more than 5 seconds.
        # We wait two times more, just in case. Overall - 10 seconds.
        wait_time_s = 10
        logger.info(
            "Waiting for {} seconds for masters to stop accepting requests".format(wait_time_s)
        )
        time.sleep(wait_time_s)
        logger.info("Masters are banned")

    def unban(self, fqdns, ignore_unbanned_masters=False):
        with Transaction(client=self._yt_client):
            for fqdn in fqdns:
                instance_banned_by_fqdn_path = "/".join(
                    [self._get_instances_banned_by_fqdn_path(), fqdn]
                )
                self._yt_client.remove(
                    instance_banned_by_fqdn_path,
                    force=ignore_unbanned_masters,
                )
        logger.info(
            "Masters are unbanned. They will start accepting queries after acquiring instance lock"
        )
