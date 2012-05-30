#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/driver )
set( SRCS
  ${BASE}/cypress_executor.cpp
  ${BASE}/executor.cpp
  ${BASE}/file_executor.cpp
  ${BASE}/main.cpp
  ${BASE}/preprocess.cpp
  ${BASE}/scheduler_executor.cpp
  ${BASE}/table_executor.cpp
  ${BASE}/transaction_executor.cpp
)

set( HDRS
  ${BASE}/config.h
  ${BASE}/cypress_executor.h
  ${BASE}/executor.h
  ${BASE}/file_executor.h
  ${BASE}/preprocess.h
  ${BASE}/scheduler_executor.h
  ${BASE}/table_executor.h
  ${BASE}/transaction_executor.h
)

add_executable( yt ${SRCS} ${HDRS} )

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt
  ${CMAKE_SOURCE_DIR}/contrib/tclap

  ${CMAKE_BINARY_DIR}
  ${CMAKE_BINARY_DIR}/include
  ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_holder
  ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_server
  ${CMAKE_BINARY_DIR}/yt/ytlib/chunk_client
  ${CMAKE_BINARY_DIR}/yt/ytlib/table_client
  ${CMAKE_BINARY_DIR}/yt/ytlib/table_server
  ${CMAKE_BINARY_DIR}/yt/ytlib/file_server
  ${CMAKE_BINARY_DIR}/yt/ytlib/transaction_server
  ${CMAKE_BINARY_DIR}/yt/ytlib/object_server
  ${CMAKE_BINARY_DIR}/yt/ytlib/cypress
  ${CMAKE_BINARY_DIR}/yt/ytlib/ytree
  ${CMAKE_BINARY_DIR}/yt/ytlib/election
  ${CMAKE_BINARY_DIR}/yt/ytlib/meta_state
)


target_link_libraries( yt
  ytlib
)

if (YT_BUILD_WITH_STLPORT)
  target_link_libraries( yt stlport )
  if (CMAKE_COMPILER_IS_GNUCXX)
    set_target_properties( yt PROPERTIES LINK_FLAGS "-nodefaultlibs -L${CMAKE_BINARY_DIR}/lib" )
  endif()
endif()

install(
  TARGETS yt
  RUNTIME DESTINATION bin
)
