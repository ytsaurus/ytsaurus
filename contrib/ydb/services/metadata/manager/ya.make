LIBRARY()

SRCS(
    abstract.cpp
    alter.cpp
    alter_impl.cpp
    table_record.cpp
    restore.cpp
    modification.cpp
    generic_manager.cpp
    preparation_controller.cpp
    restore_controller.cpp
    common.cpp
    ydb_value_operator.cpp
    modification_controller.cpp
    object.cpp
    fetch_database.cpp
)

PEERDIR(
    contrib/ydb/library/accessor
    contrib/ydb/library/actors/core
    contrib/ydb/library/table_creator
    yql/essentials/sql/settings
    contrib/ydb/public/api/protos
    contrib/ydb/core/protos
    contrib/ydb/services/bg_tasks/abstract
    contrib/ydb/services/metadata/initializer
    contrib/ydb/core/base
    contrib/ydb/services/metadata/request
)

GENERATE_ENUM_SERIALIZATION(abstract.h)

END()
