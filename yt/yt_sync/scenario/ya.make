PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    base.py
    chaos_migration.py
    clean.py
    dummy.py
    dump_diff.py
    dump_spec.py
    ensure.py
    force_compaction.py
    migrate_to_replicated.py
    move.py
    registry.py
    reshard.py
    switch_replica.py
    sync_replicas.py
)

PEERDIR(
    yt/yt_sync/action
    yt/yt_sync/core
    yt/yt_sync/core/diff
    yt/yt_sync/scenario/helpers
    yt/python/client
)

END()

RECURSE(
    helpers
)

RECURSE_FOR_TESTS(
    ut
)
