#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/api )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/private.cpp
  ${BASE}/config.cpp
  ${BASE}/admin.cpp
  ${BASE}/client.cpp
  ${BASE}/connection.cpp
  ${BASE}/transaction.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/journal_reader.cpp
  ${BASE}/journal_writer.cpp
  ${BASE}/rowset.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/admin.h
  ${BASE}/config.h
  ${BASE}/client.h
  ${BASE}/transaction.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/journal_reader.h
  ${BASE}/journal_writer.h
  ${BASE}/connection.h
  ${BASE}/rowset.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
