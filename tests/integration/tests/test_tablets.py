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

        for _ in xrange(5):
            mount_table('//tmp/t')
            unmount_table('//tmp/t')
