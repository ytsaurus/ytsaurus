#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/hive )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/cell_directory.cpp
  ${BASE}/cluster_directory.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/transaction_supervisor_service_proxy.h
  ${BASE}/config.h
  ${BASE}/cell_directory.h
  ${BASE}/cluster_directory.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_supervisor_service.proto YT_SOURCE_FILES )
