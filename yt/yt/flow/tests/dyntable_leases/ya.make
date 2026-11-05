PY3TEST()

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/tests/computation_cycles_and_buffers/lib/ya.make.inc)

# NB: no yt/yt_sync/runner here — the included recipe.inc already provides the per-flavor
# dependency (real yt_sync internally, yt_sync_mini under OPENSOURCE), and pulling the real one
# unconditionally makes the mini namespace aliases shadow it in the autocheck opensource build.
PEERDIR(
    yt/yt/flow/library/python/queue
)

REQUIREMENTS(
    cpu:4
    ram:32
)

TAG(ya:huge_logs)

# Each test starts its own local YT and its own federation, and the chaos one deliberately spends
# minutes killing and freezing processes. Together they do not fit the single chunk a MEDIUM test
# gets, so every test runs as its own chunk with its own budget.
FORK_SUBTESTS()

SPLIT_FACTOR(4)

SIZE(MEDIUM)

END()
