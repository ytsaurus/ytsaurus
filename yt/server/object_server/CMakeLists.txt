#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/object_server )
set( SRCS
  ${BASE}/attribute_set.cpp
  ${BASE}/object_detail.cpp
  ${BASE}/object_manager.cpp
  ${BASE}/object_service.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/attribute_set.h
  ${BASE}/config.h
  ${BASE}/object_detail.h
  ${BASE}/object_manager.h
  ${BASE}/object_proxy.h
  ${BASE}/object_service.h
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/type_handler.h
  ${BASE}/type_handler_detail.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/object_manager.proto YT_SOURCE_FILES )
