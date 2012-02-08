#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/object_server )
set( SRCS
  ${BASE}/attribute_set.cpp
  ${BASE}/common.cpp
  ${BASE}/id.cpp
  ${BASE}/object_detail.cpp
  ${BASE}/object_manager.cpp
)
set( HDRS
  ${BASE}/attribute_set.h
  ${BASE}/common.h
  ${BASE}/id.h
  ${BASE}/object_detail.h
  ${BASE}/object_manager.h
  ${BASE}/object_proxy.h
  ${BASE}/object_ypath_proxy.h
  ${BASE}/type_handler.h
  ${BASE}/type_handler_detail.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/object_ypath.proto YT_SOURCE_FILES )
protoc( ${BASE}/object_manager.proto YT_SOURCE_FILES )
