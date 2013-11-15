import pytest

from yt_env_setup import YTEnvSetup
from yt_commands import *

import sys

##################################################################

@pytest.mark.skipif("True")
class TestQuery(YTEnvSetup):
    NUM_MASTERS = 3
    NUM_NODES = 3
    NUM_SCHEDULERS = 0

    def test_simple(self):
        for i in xrange (1, 100):
            print "== {0} records per write ==".format(i)
            tbl = '//tmp/t{0}'.format(i)
            create('table', tbl)
            data = [ {'a' : j, 'b' : j * 10} for j in xrange(i) ]

            write(tbl, data)
            write('<append=true>{0}'.format(tbl), data)
            write('<append=true>{0}'.format(tbl), data)
            set('{0}/@schema'.format(tbl), 
                [{'name' : 'a', 'type' : 'integer'}, {'name' : 'b', 'type' : 'integer'}])

            res = select('a, b from [{0}]'.format(tbl))
            assert len(res) == 3 * i

