LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/yt/memory
    library/cpp/yt/misc

    yql/essentials/minikql
    yql/essentials/public/decimal
    yql/essentials/public/udf
    yql/essentials/types/uuid
    yql/essentials/utils

    yt/yt/client
    yt/yt/core
    yt/yt/library/decimal
)

SRCS(
    yql_ytflow_build_dict_types.cpp
    yql_ytflow_build_struct_precomputes.cpp
    yql_ytflow_convert_options.cpp
    yql_ytflow_input_codec.cpp
    yql_ytflow_member_descriptor.cpp
    yql_ytflow_output_codec.cpp
    yql_ytflow_struct_precomputes.cpp
    yql_ytflow_validate_types.cpp
    yql_ytflow_value_skipper.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
