PY3TEST()

TEST_SRCS(
    test_migrations.py
)

PEERDIR(
    devtools/ya/test/programs/test_tool/lib/migrations_config
)

END()
