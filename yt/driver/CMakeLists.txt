#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/driver )
set( SRCS
  ${BASE}/cypress_executors.cpp
  ${BASE}/etc_executors.cpp
  ${BASE}/executor.cpp
  ${BASE}/file_executors.cpp
  ${BASE}/io_helpers.cpp
  ${BASE}/journal_executors.cpp
  ${BASE}/main.cpp
  ${BASE}/operation_tracker.cpp
  ${BASE}/preprocess.cpp
  ${BASE}/scheduler_executors.cpp
  ${BASE}/table_executors.cpp
  ${BASE}/transaction_executors.cpp
)

set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_executors.h
  ${BASE}/etc_executors.h
  ${BASE}/executor.h
  ${BASE}/file_executors.h
  ${BASE}/io_helpers.h
  ${BASE}/journal_executors.h
  ${BASE}/operation_tracker.h
  ${BASE}/preprocess.h
  ${BASE}/scheduler_executors.h
  ${BASE}/table_executors.h
  ${BASE}/transaction_executors.h
)

add_executable( yt ${SRCS} ${HDRS} )

target_link_libraries( yt
  ytlib
  ytext-lfalloc
)

if (NOT WIN32)
  set_target_properties( yt
    PROPERTIES
    LINK_FLAGS "-static-libstdc++"
  )
endif()

install(
  TARGETS yt
  RUNTIME DESTINATION bin
)
