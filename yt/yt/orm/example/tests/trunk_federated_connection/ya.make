PY3TEST()

INCLUDE(../ya.make.inc)

NO_SANITIZE()

TEST_SRCS(
    ${TRUNK_TEST_FILES}

    ${ROOT_DIR}/test_aggregated_column.py # TODO(kmokrov): Move to common list after removing 23_2
    ${ROOT_DIR}/test_shared_write_lock.py # TODO(kmokrov): Move to common list after removing 23_2
    ${ROOT_DIR}/test_expressions.py # TODO(kmokrov): Move to common list after removing 23_2
)

ENV(YT_TESTS_PACKAGE_DIR=yt/yt)
ENV(EXAMPLE_USE_FEDERATED_CONNECTION=1)
ENV(EXAMPLE_MASTER_CONFIG_YSON={transaction_manager={versioned_select_enabled=%true}})

END()
