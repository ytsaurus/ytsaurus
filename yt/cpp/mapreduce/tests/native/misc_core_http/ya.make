UNITTEST_WITH_CUSTOM_ENTRY_POINT()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

EXPLICIT_DATA()

IF (OPENSOURCE)
    TAG(ya:not_autocheck)
ENDIF()

ENV(
    YT_TESTS_USE_CORE_HTTP_CLIENT="yes"
)

SRCS(
    ../misc/alter_table.cpp
    ../misc/batch_request.cpp
    ../misc/canonize_path.cpp
    ../misc/custom_client_config.cpp
    ../misc/cypress_client.cpp
    ../misc/error.cpp
    ../misc/file_io.cpp
    ../misc/format_attribute.cpp
    ../misc/lock.cpp
    ../misc/protobuf_format_derivation.cpp
    ../misc/protobuf_table_io.cpp
    ../misc/raw_io.cpp
    ../misc/redirect_stdout_to_stderr_spec_flag.cpp
    ../misc/retry_config_provider.cpp
    ../misc/schema.cpp
    ../misc/security_client.cpp
    ../misc/shutdown.cpp
    ../misc/skiff_row_table_io.cpp
    ../misc/table_io.cpp
    ../misc/tablet_client.cpp
    ../misc/temp_table.cpp
    ../misc/transactions.cpp
    ../misc/whoami.cpp
)

IF (NOT OPENSOURCE AND NOT USE_VANILLA_PROTOC)
    SRCS(../misc/tvm_auth.cpp)
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
