#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/hive )
set( SRCS
  ${BASE}/cell_directory.cpp
  ${BASE}/cluster_directory.cpp
  ${BASE}/public.cpp
  ${BASE}/private.cpp
)
set( HDRS
  ${BASE}/cell_directory.h
  ${BASE}/cluster_directory.h
  ${BASE}/config.h
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/transaction_supervisor_service_proxy.h
  ${BASE}/hive_service_proxy.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_supervisor_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/hive_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/cell_directory.proto YT_SOURCE_FILES )
