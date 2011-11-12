SET(YT_SOURCE_FILES
    common.cpp
    ytree_fwd.h
    ytree.h
    yson_format.h
    yson_events.h
    yson_reader.cpp
    yson_writer.cpp
    tree_builder.cpp
    tree_visitor.cpp
    ypath_client.cpp
    ypath_service.cpp
    node_detail.cpp
    ephemeral.cpp
    fluent.h
    json_adapter.cpp
    ypath_detail.cpp
    forwarding_yson_events.cpp
    virtual.cpp
    ytree_rpc.proto
)

include_directories(
  ${SOURCE_ROOT}/include
)

