PROTO_LIBRARY()

SET_APPEND(PROTO_FILES
    attribute_policies.proto
    author.proto
    book.proto
    cat.proto
    editor.proto
    employer.proto
    executor.proto
    genre.proto
    generic.proto
    hitchhiker.proto
    illustrator.proto
    interceptor.proto
    mother_ship.proto
    nested_columns.proto
    nexus.proto
    nirvana.proto
    database_options.proto
    publisher.proto
    tags.proto
    typographer.proto
)

SRCS(${PROTO_FILES})
LIST_PROTO(${PROTO_FILES})

PEERDIR(
    yt/yt_proto/yt/orm/client/proto
    yt/yt_proto/yt/orm/data_model
)

IF (GO_PROTO)
    PEERDIR(
        yt/go/proto/core/yson
        yt/go/proto/core/ytree
    )
ELSE()
    PEERDIR(yt/yt_proto/yt/core)
ENDIF()

END()
