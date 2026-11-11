import yatest.common

from yt.yql.tests.sql.runners.dq_file import collect_test_classes


globals().update(collect_test_classes(yatest.common.source_path(__file__)))
