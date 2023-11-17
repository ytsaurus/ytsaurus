LIBRARY()

SRCS(
    codec.h
    compare.h
    comp_factory.h
    context.h
    interface.h
    interface.cpp
    pack.h
    parser.h
    type_desc.h
    utils.h
)

PEERDIR(
    util
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/arrow
    contrib/ydb/library/yql/core/cbo
)

YQL_LAST_ABI_VERSION()

END()
