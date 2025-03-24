LIBRARY()

SRCS(
    scheme_borders.cpp
    scheme_tablecell.cpp
    scheme_tabledefs.cpp
    scheme_types_defs.cpp
    scheme_type_info.cpp
    scheme_types_proto.cpp
    scheme_pathid.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/charset/lite
    library/cpp/containers/bitseq
    library/cpp/deprecated/enum_codegen
    library/cpp/yson
    contrib/ydb/public/api/protos
    contrib/ydb/core/scheme/protos
    contrib/ydb/core/scheme_types
    contrib/ydb/library/aclib
    yql/essentials/parser/pg_wrapper/interface
    contrib/ydb/public/lib/scheme_types
    # temporary.
    contrib/ydb/library/pretty_types_print/protobuf
    library/cpp/lwtrace/mon
    library/cpp/containers/absl_flat_hash
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_pg
)
