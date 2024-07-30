UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

EXPLICIT_DATA()

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

SRCS(
    alter_table.cpp
    batch_request.cpp
    canonize_path.cpp
    custom_client_config.cpp
    cypress_client.cpp
    error.cpp
    file_io.cpp
    format_attribute.cpp
    lock.cpp
    protobuf_format_derivation.cpp
    protobuf_table_io.cpp
    raw_io.cpp
    redirect_stdout_to_stderr_spec_flag.cpp
    retry_config_provider.cpp
    schema.cpp
    security_client.cpp
    shutdown.cpp
    skiff_row_table_io.cpp
    table_io.cpp
    tablet_client.cpp
    temp_table.cpp
    transactions.cpp
    whoami.cpp
)

IF (NOT OPENSOURCE AND NOT USE_VANILLA_PROTOC)
    SRCS(tvm_auth.cpp)
ENDIF()

PEERDIR(
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/cpp/mapreduce/interface
    yt/cpp/mapreduce/tests/native/proto_lib
    yt/cpp/mapreduce/tests/yt_unittest_lib
    yt/cpp/mapreduce/tests/gtest_main
    yt/cpp/mapreduce/util
)

SIZE(MEDIUM)

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(5)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

END()
