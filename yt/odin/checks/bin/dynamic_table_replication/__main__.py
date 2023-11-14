from yt_odin_checks.lib.check_runner import main

from yt.wrapper.client import Yt

from datetime import datetime
from collections import defaultdict
import calendar

META = 'markov'
PATH = '//sys/admin/odin/replicated_table_{}'


class YtWrapper(object):
    def __init__(self, _yt_client, logger):
        self._yt_client = _yt_client
        self.logger = logger

    def __getattr__(self, name):
        def _yt(method, *args, **kwargs):
            joined_args = ', '.join(map(lambda a: "'%s'" % a, args))
            if kwargs:
                self.logger.debug('yt.{}({}, **{})'.format(method, joined_args, kwargs))
                return getattr(self._yt_client, method)(*args, **kwargs)
            else:
                self.logger.debug('yt.{}({})'.format(method, joined_args))
                return getattr(self._yt_client, method)(*args)
        return _yt.__get__(name)


def run_check(yt_client, logger, options, states):
    def iter_modes(func):
        for mode in ['async', 'sync']:
            path = PATH.format(mode)
            if mode == 'sync':
                path = "{}_{}".format(path, options["cluster_name"])
            func(mode, path)

    def insert_rows(mode, path):
        if path in alerts:
            return
        row = {}
        dt = datetime.utcnow()
        row['k'] = int(calendar.timegm(dt.timetuple()) * 1000000 + dt.microsecond)
        row['v'] = 42
        opts = {'raw': False}
        if mode == 'async':
            opts['require_sync_replica'] = False
        yt.insert_rows(path, [row], **opts)

    def precheck(mode, path):
        if not yt.exists(path):
            alerts[path] = {'Table does not exist': {}}
            return
        tablet_state = yt.get('{}/@tablet_state'.format(path))
        if not tablet_state == 'mounted':
            alerts[path] = {'Table tablets are not mounted: @tablet_state': tablet_state}
            return
        replicas_path = '{}/@replicas'.format(path)
        replicas = yt.get(replicas_path).items()
        not_enabled = dict(filter(lambda k: k[-1]["state"] != "enabled", replicas))
        if len(not_enabled) > 0:
            alerts[path] = {"Not all replics are enabled": not_enabled}

    def check_replicas(mode, path):
        if path in alerts:
            return
        replicas_path = '{}/@replicas'.format(path)
        replicas = yt.get(replicas_path).items()
        clusters = map(lambda k: k[-1]['cluster_name'], replicas)
        if options['cluster_name'] not in clusters:
            alerts[path] = {'No {} replica in: {}'.format(options['cluster_name'], replicas_path): clusters}
            return

        local_replica_id = [replica_id for replica_id, values in replicas if values['cluster_name'] == options['cluster_name']]
        assert len(local_replica_id) == 1
        local_replica_id = local_replica_id[0]

        replication_errors = defaultdict(list)
        tablet_infos = yt.get('{}/@tablets'.format(path))
        for tablet_info in tablet_infos:
            if tablet_info['replication_error_count'] == 0:
                continue

            peers = yt.get('#{}/@peers'.format(tablet_info['cell_id']))
            hosts = [peer["address"] for peer in peers if peer["state"] == "leading"]
            if len(hosts) == 0:
                msg = 'No peer for cell {} is in leading state (replication_error_count on tablet: {})'
                alerts[path] = {msg.format(tablet_info['cell_id'], tablet_info['replication_error_count']): options["cluster_name"]}
                return

            tablet_replication_errors = yt.get(
                '//sys/cluster_nodes/{host}/orchid/tablet_cells/{cell_id}/tablets/{tablet_id}/replication_errors'.format(
                    host=hosts[0],
                    cell_id=tablet_info['cell_id'],
                    tablet_id=tablet_info['tablet_id']))

            for replica_id, replica_error in tablet_replication_errors.items():
                if replica_id != local_replica_id:
                    continue
                replication_errors[replica_id].append(replica_error)

        if len(replication_errors) > 0:
            alerts[path] = {'errors': dict(replication_errors)}

        for replica_id, attrs in replicas:
            if attrs['cluster_name'] == options['cluster_name']:
                if attrs['replication_lag_time'] > 0:
                    if attrs['replication_lag_time'] > max_lag_time:
                        alerts[path] = {'replication_lag_time': attrs['replication_lag_time']}

    alerts = {}
    yt = YtWrapper(Yt(proxy=META, token=yt_client.config["token"]), logger)

    iter_modes(precheck)

    max_lag_time = options['max_lag_time'] if 'max_lag_time' in options else 42 * 60 * 1000

    iter_modes(insert_rows)
    iter_modes(check_replicas)

    if len(alerts) > 0:
        logger.info(alerts)
        crits = map(lambda k: 'errors' in k[-1] or 'replication_lag_time' in k[-1], alerts.items())
        state = states.UNAVAILABLE_STATE if any(crits) else states.PARTIALLY_AVAILABLE_STATE
        return state, str(alerts)
    else:
        return states.FULLY_AVAILABLE_STATE


if __name__ == "__main__":
    main(run_check)
