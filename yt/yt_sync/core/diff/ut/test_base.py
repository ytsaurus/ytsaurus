from yt.yt_sync.core.diff import NodeDiffType
from yt.yt_sync.core.diff import TableDiffType


def test_diff_types_not_intersecting():
    assert not TableDiffType.all().intersection(NodeDiffType.all())
