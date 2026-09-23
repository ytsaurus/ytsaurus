PROTO_LIBRARY()

WITHOUT_LICENSE_TEXTS()

VERSION(4.2.0)

LICENSE(Apache-2.0)

PROTO_NAMESPACE(contrib/python/pyspark)

PY_NAMESPACE(pyspark.sql.connect.proto)

GRPC()

EXCLUDE_TAGS(GO_PROTO)

SRCS(
    base.proto
    catalog.proto
    commands.proto
    common.proto
    example_plugins.proto
    expressions.proto
    ml.proto
    ml_common.proto
    pipelines.proto
    relations.proto
    types.proto
)

END()
