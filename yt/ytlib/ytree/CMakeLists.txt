SET(YT_SOURCE_FILES
    common.cpp

    ytree_fwd.h

    ytree.h

    yson_format.h

    yson_events.h
    forwarding_yson_events.cpp
    null_yson_consumer.cpp

    yson_reader.cpp
    yson_writer.cpp

    tree_builder.cpp
    tree_visitor.cpp

    node_detail.cpp
    ephemeral.cpp

    fluent.h

    json_adapter.cpp

    virtual.cpp

    ypath_client.cpp
    ypath_client-inl.h

    ypath_service.cpp
    ypath_detail.cpp
    ypath_rpc.h
    ypath_rpc.proto

    yson_file_service.cpp
)

include_directories(
  ${SOURCE_ROOT}/include
)

