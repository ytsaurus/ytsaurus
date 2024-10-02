PY3TEST()

INCLUDE(../ya.make.inc)

TEST_SRCS(
    ${TRUNK_TEST_FILES}

    ${ROOT_DIR}/test_aggregated_column.py # TODO(kmokrov): Move to common list after removing 23_2
    ${ROOT_DIR}/test_shared_write_lock.py # TODO(kmokrov): Move to common list after removing 23_2
    ${ROOT_DIR}/test_expressions.py # TODO(kmokrov): Move to common list after removing 23_2
)

ENV(YT_TESTS_PACKAGE_DIR=yt/yt)
ENV(EXAMPLE_MASTER_CONFIG_YSON={transaction_manager={versioned_select_enabled=%true}})

# DEVTOOLSSUPPORT-47333.
NO_BUILD_IF(OPENSOURCE)

END()
