import os
import mypy.test.config as config

from yatest.common import work_path

config.PREFIX = work_path()
config.test_data_prefix = os.path.join(config.PREFIX, 'test-data', 'unit')
config.package_path = os.path.join(config.PREFIX, 'test-data', 'packages')
config.test_temp_dir = 'tmp'

assert os.path.isdir(config.test_data_prefix), \
    'Test data prefix ({}) not set correctly'.format(config.test_data_prefix)


from mypy.test.testargs import *  # noqa
from mypy.test.testcheck import *  # noqa
# from mypy.test.testcmdline import *  # noqa
# from mypy.test.testdaemon import *  # noqa
from mypy.test.testdeps import *  # noqa
from mypy.test.testdiff import *  # noqa
# from mypy.test.testerrorstream import *  # noqa
from mypy.test.testfinegrained import *  # noqa
from mypy.test.testfinegrainedcache import *  # noqa
from mypy.test.testgraph import *  # noqa
from mypy.test.testinfer import *  # noqa
from mypy.test.testipc import *  # noqa
from mypy.test.testmerge import *  # noqa
from mypy.test.testmodulefinder import *  # noqa
from mypy.test.testmoduleinfo import *  # noqa
from mypy.test.testmypyc import *  # noqa
# from mypy.test.testparse import *  # noqa
# from mypy.test.testpep561 import *  # noqa
# from mypy.test.testpythoneval import *  # noqa
from mypy.test.testreports import *  # noqa
# from mypy.test.testsamples import *  # noqa
# from mypy.test.testsemanal import *  # noqa
from mypy.test.testsolve import *  # noqa
from mypy.test.teststubgen import *  # noqa
from mypy.test.testsubtypes import *  # noqa
# from mypy.test.testtransform import *  # noqa
from mypy.test.testtypegen import *  # noqa
from mypy.test.testtypes import *  # noqa
