SET(YT_SOURCE_FILES
    common.h
    ytree_fwd.h
    ytree.h
    yson_format.h
    yson_events.h
    yson_reader.cpp
    yson_writer.cpp
    tree_builder.h
    tree_visitor.h
    ypath.cpp
    node.cpp
    ephemeral.cpp
    fluent.h
    json_adapter.cpp
)

include_directories(
  ${SOURCE_ROOT}/include
)

