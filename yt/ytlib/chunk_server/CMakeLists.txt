#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/chunk_server )
set( SRCS
  ${BASE}/chunk.cpp
  ${BASE}/chunk_list.cpp
  ${BASE}/chunk_manager.cpp
  ${BASE}/chunk_placement.cpp
  ${BASE}/chunk_replication.cpp
  ${BASE}/chunk_service.cpp
  ${BASE}/common.cpp
  ${BASE}/cypress_integration.cpp
  ${BASE}/holder.cpp
  ${BASE}/holder_authority.cpp
  ${BASE}/holder_lease_tracker.cpp
  ${BASE}/holder_statistics.cpp
  ${BASE}/job.cpp
  ${BASE}/job_list.cpp
)
set( HDRS
  ${BASE}/chunk.h
  ${BASE}/chunk_list.h
  ${BASE}/chunk_manager.h
  ${BASE}/chunk_placement.h
  ${BASE}/chunk_replication.h
  ${BASE}/chunk_service.h
  ${BASE}/chunk_service_proxy.h
  ${BASE}/common.h
  ${BASE}/config.h
  ${BASE}/cypress_integration.h
  ${BASE}/holder.h
  ${BASE}/holder_authority.h
  ${BASE}/holder_lease_tracker.h
  ${BASE}/holder_statistics.h
  ${BASE}/job.h
  ${BASE}/job_list.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/chunk_manager.proto YT_SOURCE_FILES )
protoc( ${BASE}/chunk_service.proto YT_SOURCE_FILES )
