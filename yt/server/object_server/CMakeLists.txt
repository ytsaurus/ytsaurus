#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/object_server )
set( SRCS
  ${BASE}/attribute_set.cpp
  ${BASE}/gc.cpp
  ${BASE}/master.cpp
  ${BASE}/object.cpp
  ${BASE}/staged_object.cpp
  ${BASE}/object_detail.cpp
  ${BASE}/object_manager.cpp
  ${BASE}/object_service.cpp
  ${BASE}/private.cpp
  ${BASE}/schema.cpp
  ${BASE}/type_handler.cpp
  ${BASE}/master_cache_service.cpp
)
set( HDRS
  ${BASE}/attribute_set.h
  ${BASE}/config.h
  ${BASE}/gc.h
  ${BASE}/master.h
  ${BASE}/object.h
  ${BASE}/staged_object.h
  ${BASE}/object_detail.h
  ${BASE}/object_manager.h
  ${BASE}/object_proxy.h
  ${BASE}/object_service.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/schema.h
  ${BASE}/type_handler.h
  ${BASE}/type_handler_detail.h
  ${BASE}/master_cache_service.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/object_manager.proto YT_SOURCE_FILES )
