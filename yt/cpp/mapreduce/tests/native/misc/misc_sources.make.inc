SET(MISC_SRCS_ROOT ${ARCADIA_ROOT}/yt/cpp/mapreduce/tests/native/misc)

SET(MISC_SRCS
    ${MISC_SRCS_ROOT}/alter_table.cpp
    ${MISC_SRCS_ROOT}/enable_debug_command_line_arguments_spec_flag.cpp
    ${MISC_SRCS_ROOT}/batch_request.cpp
    ${MISC_SRCS_ROOT}/canonize_path.cpp
    ${MISC_SRCS_ROOT}/custom_client_config.cpp
    ${MISC_SRCS_ROOT}/cypress_client.cpp
    ${MISC_SRCS_ROOT}/error.cpp
    ${MISC_SRCS_ROOT}/file_io.cpp
    ${MISC_SRCS_ROOT}/format_attribute.cpp
    ${MISC_SRCS_ROOT}/lock.cpp
    ${MISC_SRCS_ROOT}/protobuf_format_derivation.cpp
    ${MISC_SRCS_ROOT}/protobuf_table_io.cpp
    ${MISC_SRCS_ROOT}/raw_io.cpp
    ${MISC_SRCS_ROOT}/redirect_stdout_to_stderr_spec_flag.cpp
    ${MISC_SRCS_ROOT}/retry_config_provider.cpp
    ${MISC_SRCS_ROOT}/schema.cpp
    ${MISC_SRCS_ROOT}/security_client.cpp
    ${MISC_SRCS_ROOT}/shutdown.cpp
    ${MISC_SRCS_ROOT}/skiff_row_table_io.cpp
    ${MISC_SRCS_ROOT}/table_io.cpp
    ${MISC_SRCS_ROOT}/tablet_client.cpp
    ${MISC_SRCS_ROOT}/temp_table.cpp
    ${MISC_SRCS_ROOT}/transactions.cpp
    ${MISC_SRCS_ROOT}/whoami.cpp
)

IF (NOT OPENSOURCE AND NOT USE_VANILLA_PROTOC)
    SET(MISC_SRCS ${MISC_SRCS} ${MISC_SRCS_ROOT}/tvm_auth.cpp)
ENDIF()
