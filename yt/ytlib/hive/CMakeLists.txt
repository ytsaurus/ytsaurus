#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/hive )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/remote_timestamp_provider.cpp
  ${BASE}/cell_directory.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/transaction_supervisor_service_proxy.h
  ${BASE}/config.h
  ${BASE}/timestamp_provider.h
  ${BASE}/timestamp_service_proxy.h
  ${BASE}/remote_timestamp_provider.h
  ${BASE}/cell_directory.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/transaction_supervisor_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/timestamp_service.proto YT_SOURCE_FILES )
