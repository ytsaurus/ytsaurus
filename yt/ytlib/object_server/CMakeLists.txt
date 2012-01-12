#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/object_server )
set( SRCS
  ${BASE}/common.cpp
  ${BASE}/id.cpp
  ${BASE}/object.cpp
  ${BASE}/object_manager.cpp
)
set( HDRS
  ${BASE}/common.h
  ${BASE}/id.h
  ${BASE}/object.h
  ${BASE}/object_manager.h
  ${BASE}/type_handler.h
  ${BASE}/type_handler_detail.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
