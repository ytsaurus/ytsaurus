#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/job_proxy )
set( SRCS
  ${BASE}/job_detail.cpp
  ${BASE}/job_proxy.cpp
  ${BASE}/map_job_io.cpp
  ${BASE}/ordered_merge_job.cpp
  ${BASE}/partition_job.cpp
  ${BASE}/partition_sort_job.cpp
  ${BASE}/pipes.cpp
  ${BASE}/private.cpp
  ${BASE}/reduce_job_io.cpp
  ${BASE}/simple_sort_job.cpp
  ${BASE}/small_key.cpp
  ${BASE}/sorted_merge_job.cpp
  ${BASE}/stderr_output.cpp
  ${BASE}/table_output.cpp
  ${BASE}/user_job.cpp
  ${BASE}/user_job_io.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/job.h
  ${BASE}/job_detail.h
  ${BASE}/job_proxy.h
  ${BASE}/map_job_io.h
  ${BASE}/ordered_merge_job.h
  ${BASE}/partition_job.h
  ${BASE}/partition_sort_job.h
  ${BASE}/pipes.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/reduce_job_io.h
  ${BASE}/simple_sort_job.h
  ${BASE}/small_key.h
  ${BASE}/sorted_merge_job.h
  ${BASE}/stderr_output.h
  ${BASE}/table_output.h
  ${BASE}/user_job.h
  ${BASE}/user_job_io.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

#PROTOC( ${BASE}/job_stats.proto YT_SOURCE_FILES )