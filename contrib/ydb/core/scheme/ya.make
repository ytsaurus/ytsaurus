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
    library/cpp/actors/core
    library/cpp/charset
    library/cpp/containers/bitseq
    library/cpp/deprecated/enum_codegen
    library/cpp/yson
    contrib/ydb/core/scheme/protos
    contrib/ydb/core/scheme_types
    contrib/ydb/library/aclib
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/public/lib/scheme_types
    # temporary.
    contrib/ydb/library/pretty_types_print/protobuf
    library/cpp/lwtrace/mon
)

END()

RECURSE_FOR_TESTS(
    ut
)
