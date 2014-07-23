import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep

##################################################################

class TestTablets(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 0

    def _wait(self, predicate):
        while not predicate():
            sleep(0.1)

    def _sync_create_cells(self, size, count):
        ids = []
        for _ in xrange(count):
            ids.append(create_tablet_cell(size))

        print 'Waiting for tablet cells to become healthy...'
        self._wait(lambda : all(get('//sys/tablet_cells/' + id + '/@health') == 'good'
                                for id in ids))

    def _create_table(self):
        create('table', '//tmp/t',
               attributes = {
                 'schema': [{'name': 'key', 'type': 'int64'}, {'name': 'value', 'type': 'string'}],
                 'key_columns': ['key']
               })

    def _get_tablet_leader_address(self, tablet_id):
        cell_id = get('//sys/tablets/' + tablet_id + '/@cell_id')
        peers = get('//sys/tablet_cells/' + cell_id + '/@peers')
        leader_peer = list(x for x in peers if x['state'] == 'leading')[0]
        return leader_peer['address']
 
    def _find_tablet_orchid(self, address, tablet_id):
        slots = get('//sys/nodes/' + address + '/orchid/tablet_slots')
        for slot in slots:
            if slot['state'] == 'leading':
                tablets = slot['tablets']
                if tablet_id in tablets:
                    return tablets[tablet_id]
        return None

    def _sync_mount_table(self, path):
        mount_table(path)

        print 'Waiting for tablets for become mounted...'
        
        def _check():
            tablets = get('//tmp/t/@tablets')
            return all(x['state'] == 'mounted' for x in tablets)

        self._wait(_check)
                

    def _get_pivot_keys(self, path):
        tablets = get(path + '/@tablets')
        return [tablet['pivot_key'] for tablet in tablets]
           

    def test_mount1(self):
        self._sync_create_cells(1, 1)
        self._create_table()

        mount_table('//tmp/t')
        tablets = get('//tmp/t/@tablets')
        assert len(tablets) == 1
        tablet_id = tablets[0]['tablet_id']
        cell_id = tablets[0]['cell_id']

        tablet_ids = get('//sys/tablet_cells/' + cell_id + '/@tablet_ids')
        assert tablet_ids == [tablet_id]


    def test_unmount1(self):
        self._sync_create_cells(1, 1)
        self._create_table()

        mount_table('//tmp/t')

        tablets = get('//tmp/t/@tablets')
        assert len(tablets) == 1

        tablet = tablets[0]
        assert tablet["pivot_key"] == []

        print 'Waiting for table to become mounted...'
        while get('//tmp/t/@tablets/0/state') != 'mounted':
            sleep(0.1)

        unmount_table('//tmp/t')

        print 'Waiting for table to become unmounted...'
        while get('//tmp/t/@tablets/0/state') != 'unmounted':
            sleep(0.1)

    def test_reshard_unmounted(self):
        self._sync_create_cells(1, 1)
        self._create_table()

        reshard_table('//tmp/t', [[]])
        assert self._get_pivot_keys('//tmp/t') == [[]]

        reshard_table('//tmp/t', [[], [100]])
        assert self._get_pivot_keys('//tmp/t') == [[], [100]]

        with pytest.raises(YtError): reshard_table('//tmp/t', [[], []])
        assert self._get_pivot_keys('//tmp/t') == [[], [100]]

        reshard_table('//tmp/t', [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys('//tmp/t') == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table('//tmp/t', [[101]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys('//tmp/t') == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table('//tmp/t', [[300]], first_tablet_index=3, last_tablet_index=3)
        assert self._get_pivot_keys('//tmp/t') == [[], [100], [200]]

        with pytest.raises(YtError): reshard_table('//tmp/t', [[100], [200]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys('//tmp/t') == [[], [100], [200]]

        reshard_table('//tmp/t', [[100], [150], [200]], first_tablet_index=1, last_tablet_index=2)
        assert self._get_pivot_keys('//tmp/t') == [[], [100], [150], [200]]

        with pytest.raises(YtError): reshard_table('//tmp/t', [[100], [100]], first_tablet_index=1, last_tablet_index=1)
        assert self._get_pivot_keys('//tmp/t') == [[], [100], [150], [200]]

    def test_force_unmount_on_remove(self):
        self._sync_create_cells(1, 1)
        self._create_table()
        self._sync_mount_table('//tmp/t')

        tablet_id = get('//tmp/t/@tablets/0/tablet_id')
        address = self._get_tablet_leader_address(tablet_id)
        assert self._find_tablet_orchid(address, tablet_id) is not None
       
        remove('//tmp/t')
        sleep(0.1)
        assert self._find_tablet_orchid(address, tablet_id) is None
         
