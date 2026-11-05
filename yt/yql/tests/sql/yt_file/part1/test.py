import yatest.common

from yt.yql.tests.sql.runners.yt_file import collect_test_classes


globals().update(collect_test_classes(yatest.common.source_path(__file__)))
