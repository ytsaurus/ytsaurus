from yt_odin_checks.lib.chyt_clique_liveness import represent_error
from yt.common import YtError


def test_error_representation():
    aborted_job_error = \
        {u'attributes': {u'datetime': u'2020-01-17T16:22:45.938929Z',
                         u'fid': 18444246599381045161,
                         u'host': u'sas4-1256-node-hahn.sas.yp-c.yandex.net',
                         u'pid': 149,
                         u'resource_delta': u'{UserSlots: 0, Cpu: 0.000000, Gpu: 0, UserMemory: 3539, SystemMemory: 0, Network: 0'
                                            'ReplicationSlots: 0, ReplicationDataSize: 0, RemovalSlots: 0, RepairSlots: 0, RepairDataSize: 0'
                                            'SealSlots: 0}',
                         u'span_id': 12941808211961713857,
                         u'tid': 10777316356054208069,
                         u'trace_id': u'a1eed96a-9cd89302-ae6f2990-462da324'},
         u'code': 1102,
         u'message': u'Failed to increase resource usage'}
    assert represent_error(YtError(**aborted_job_error)) == "Failed to increase resource usage"

    oom_by_watchdog_error = \
        {u'attributes': {u'datetime': u'2020-01-21T16:32:26.170286Z',
                         u'fid': 18446469435963019696,
                         u'host': u'sas3-9054-node-hahn.sas.yp-c.yandex.net',
                         u'pid': 222033,
                         u'tid': 17487340927829755119},
         u'code': 1205,
         u'inner_errors': [{u'attributes': {u'datetime': u'2020-01-21T16:32:20.726815Z',
                                            u'exit_code': 42,
                                            u'fid': 0,
                                            u'host': u'(unknown)',
                                            u'pid': 2,
                                            u'tid': 12616593985321784421},
                            u'code': 10000,
                            u'message': u'Process exited with code 42'}],
         u'message': u'User job failed'}
    assert represent_error(YtError(**oom_by_watchdog_error)) == "Process exited with code 42 [OOM by watchdog]"

    other_signal_error = \
        {u'attributes': {u'datetime': u'2020-01-15T12:56:00.831684Z',
                         u'fid': 18446469538197378346,
                         u'host': u'sas1-9977-node-hahn.sas.yp-c.yandex.net',
                         u'pid': 977764,
                         u'tid': 6340804421093416111},
         u'code': 1205,
         u'inner_errors': [{u'attributes': {u'datetime': u'2020-01-15T12:55:55.634370Z',
                                            u'exit_code': 250,
                                            u'fid': 0,
                                            u'host': u'(unknown)',
                                            u'pid': 2,
                                            u'tid': 17023767677557954387},
                            u'code': 10000,
                            u'message': u'Process exited with code 250'},
                           {u'attributes': {u'core_infos': [{u'executable_name': u'Main',
                                                             u'process_id': 54,
                                                             u'size': 164031811584}],
                                            u'datetime': u'2020-01-15T12:56:00.831670Z',
                                            u'fid': 18446469538197378346,
                                            u'host': u'sas1-9977-node-hahn.sas.yp-c.yandex.net',
                                            u'pid': 977764,
                                            u'tid': 6340804421093416111},
                            u'code': 1,
                            u'message': u'User job produced core files'}],
         u'message': u'User job failed'}
    assert represent_error(YtError(**other_signal_error)) == "Process exited with code 250 [core dumped] [SIGABRT]"

    oom_error = \
        {u'attributes': {u'datetime': u'2020-01-17T15:02:35.296140Z',
                         u'fid': 18446469521326040341,
                         u'host': u'sas4-0804-node-hahn.sas.yp-c.yandex.net',
                         u'pid': 869207,
                         u'tid': 9427570015128836471},
         u'code': 1205,
         u'inner_errors': [{u'attributes': {u'datetime': u'2020-01-17T15:02:21.351969Z',
                                            u'fid': 18446459779082774371,
                                            u'host': u'sas4-0804-node-hahn.sas.yp-c.yandex.net',
                                            u'limit': 66000000000,
                                            u'pid': 869207,
                                            u'rss': 66330099712,
                                            u'tid': 348362623341263696,
                                            u'tmpfs': []},
                            u'code': 1200,
                            u'message': u'Memory limit exceeded'}],
         u'message': u'User job failed'}
    assert represent_error(YtError(**oom_error)) == "OOM"
