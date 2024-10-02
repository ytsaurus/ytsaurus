PROTO_LIBRARY()

# See https://clubs.at.yandex-team.ru/arcadia/27574.
SET(PROTOC_TRANSITIVE_HEADERS "no")

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/codegen/config.inc)
INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/codegen/generator/build/data_model_proto.make.inc)

END()
