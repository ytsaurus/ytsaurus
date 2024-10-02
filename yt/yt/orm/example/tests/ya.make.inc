INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/python/ya_programs.make.inc)

DEPENDS(
    ${ARCADIA_ROOT}/yt/yt/orm/example/python/recipe/bin
)

IF (NOT OPENSOURCE) 
    DEPENDS(
        ${ARCADIA_ROOT}/yt/yt/orm/example/tools/touch_index
        ${ARCADIA_ROOT}/yt/yt/orm/example/tools/migrate_attributes
        ${ARCADIA_ROOT}/yt/yt/orm/tools/history/trim_history
        ${ARCADIA_ROOT}/yt/yt/orm/tools/history/update_history_index
        ${ARCADIA_ROOT}/yt/yt/orm/tools/history/history_migrator
        ${ARCADIA_ROOT}/yt/yt/orm/tools/migration/slicer
    )
ENDIF()

PEERDIR(
    yt/yt/orm/example/python/admin
    yt/yt/orm/example/python/client
    yt/yt/orm/example/python/local

    yt/yt/orm/python/orm/admin
    yt/yt/orm/python/orm/tests

    yt/yt/python/yt_driver_rpc_bindings

    yt/python/yt/test_helpers
    yt/python/yt/wrapper
    yt/python/yt/yson

    contrib/python/pytest-lazy-fixtures
)

SET(ROOT_DIR yt/yt/orm/example/tests)

SET(TEST_FILES
    ${ROOT_DIR}/conftest.py

    ${ROOT_DIR}/test_access_control.py
    ${ROOT_DIR}/test_aggregate_objects.py
    ${ROOT_DIR}/test_attribute_policy.py
    ${ROOT_DIR}/test_attributes_validation.py
    ${ROOT_DIR}/test_attributes_updatable_by_superuser_only.py
    ${ROOT_DIR}/test_author.py
    ${ROOT_DIR}/test_backup.py
    ${ROOT_DIR}/test_book.py
    ${ROOT_DIR}/test_client.py
    ${ROOT_DIR}/test_composite.py
    ${ROOT_DIR}/test_custom_base_type_handler.py
    ${ROOT_DIR}/test_executor.py
    ${ROOT_DIR}/test_finalizers.py
    ${ROOT_DIR}/test_full_scan.py
    ${ROOT_DIR}/test_group.py
    ${ROOT_DIR}/test_genre.py
    ${ROOT_DIR}/test_history_api.py
    ${ROOT_DIR}/test_history_migration.py
    ${ROOT_DIR}/test_history_operations.py
    ${ROOT_DIR}/test_incomplete_base.py
    ${ROOT_DIR}/test_index_building.py
    ${ROOT_DIR}/test_index_engine.py
    ${ROOT_DIR}/test_index_using.py
    ${ROOT_DIR}/test_index_with_predicate.py
    ${ROOT_DIR}/test_interceptor.py
    ${ROOT_DIR}/test_lock.py
    ${ROOT_DIR}/test_many_to_one_view.py
    ${ROOT_DIR}/test_many_to_many.py
    ${ROOT_DIR}/test_many_to_many_view.py
    ${ROOT_DIR}/test_methods.py
    ${ROOT_DIR}/test_migrations.py
    ${ROOT_DIR}/test_mother_ship.py
    ${ROOT_DIR}/test_nexus.py
    ${ROOT_DIR}/test_nested_columns.py
    ${ROOT_DIR}/test_one_to_many.py
    ${ROOT_DIR}/test_order_by.py
    ${ROOT_DIR}/test_partial_result.py
    ${ROOT_DIR}/test_performance.py
    ${ROOT_DIR}/test_proto_reference.py
    ${ROOT_DIR}/test_publisher.py
    ${ROOT_DIR}/test_read_uncommitted.py
    ${ROOT_DIR}/test_revision.py
    ${ROOT_DIR}/test_schema.py
    ${ROOT_DIR}/test_semaphore.py
    ${ROOT_DIR}/test_semaphore_set.py
    ${ROOT_DIR}/test_storage_type.py
    ${ROOT_DIR}/test_sweep.py
    ${ROOT_DIR}/test_typographer.py
    ${ROOT_DIR}/test_update_policy.py
    ${ROOT_DIR}/test_user.py
    ${ROOT_DIR}/test_validation.py
    ${ROOT_DIR}/test_watch_log.py
    ${ROOT_DIR}/test_watch_log_consumer.py
    ${ROOT_DIR}/test_yson_extract.py
    ${ROOT_DIR}/test_attribute_migration.py
    ${ROOT_DIR}/test_attribute_migration_operation.py
)

SET(TRUNK_TEST_FILES
    ${TEST_FILES}
    ${ROOT_DIR}/test_select_timestamp.py # TODO(grigminakov): Add to 24_1 after updating YT resource.
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/tests/ya_make_integration.inc)

REQUIREMENTS(
    ram_disk:4
    cpu:4
    ram:32
)

IF (AUTOCHECK OR RELEASE_MACHINE OR YT_TEAMCITY)
    # Treat test files independently.
    FORK_TEST_FILES()
    # Treat tests within one test suite independently.
    FORK_SUBTESTS()
    IF (SANITIZER_TYPE)
        TAG(ya:yt ya:fat ya:huge_logs)
        SIZE(LARGE)
        SPLIT_FACTOR(10)
    ELSE()
        SPLIT_FACTOR(5)
    ENDIF()
ENDIF()
