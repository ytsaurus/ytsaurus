from yt_commands import authors

from gdb_test_lib import get_core, analyze

import re


@authors("babenko")
def test_tcmalloc_block():
    ctx, path = get_core()
    # The tcmalloc page-map walk must snap an interior pointer back to the start
    # of its allocation and report the byte offset within it.
    out = analyze(ctx, path, "yt-tcmalloc-block (char*)GdbCycleHeadAddress + 16")
    assert "start" in out
    assert re.search(r"offset\s+\+0x10", out)


@authors("babenko")
def test_tcmalloc_block_large():
    ctx, path = get_core()
    # A multi-MB allocation exceeds the largest size class, so tcmalloc serves it
    # as a single (size-class 0) span; an interior pointer must snap to the span
    # start with the right offset, reported as a large/single span.
    out = analyze(ctx, path, "yt-tcmalloc-block (char*)GdbLargeAllocAddress + 0x4000")
    assert re.search(r"offset\s+\+0x4000", out)
    assert "large" in out
