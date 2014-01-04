import pytest
import sys

import yt.yson

from yt_env_setup import YTEnvSetup
from yt_commands import *
from time import sleep

##################################################################

class TestTablets(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 5
    NUM_SCHEDULERS = 0

    def _create_cells(self, size, count):
        ids = []
        for _ in xrange(count):
            ids.append(create_tablet_cell(size))

        print 'Waiting for tablet cells to become healthy...'
        while any(get('//sys/tablet_cells/' + id + '/@health') != 'good' for
                  id in ids):
            sleep(0.1)

    def _create_table(self):
        create('table', '//tmp/t',
               attributes = {
                 'schema': [{'name': 'key', 'type': 'integer'}, {'name': 'value', 'type': 'string'}],
                 'key_columns': ['key']
               })

    def _get_pivot_keys(self, path):
        tablets = get(path + '/@tablets')
        return [tablet['pivot_key'] for tablet in tablets]
           

    def test_mount1(self):
        self._create_cells(1, 1)
        self._create_table()

        mount_table('//tmp/t')
        tablets = get('//tmp/t/@tablets')
        assert len(tablets) == 1
        tablet_id = tablets[0]['tablet_id']
        cell_id = tablets[0]['cell_id']

        tablet_ids = get('//sys/tablet_cells/' + cell_id + '/@tablet_ids')
        assert tablet_ids == [tablet_id]


    def test_unmount1(self):
        self._create_cells(1, 1)
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
        self._create_cells(1, 1)
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
