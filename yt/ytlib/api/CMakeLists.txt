#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/api )
set( SRCS
  ${BASE}/admin.cpp
  ${BASE}/client.cpp
  ${BASE}/config.cpp
  ${BASE}/connection.cpp
  ${BASE}/file_reader.cpp
  ${BASE}/file_writer.cpp
  ${BASE}/journal_reader.cpp
  ${BASE}/journal_writer.cpp
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/rowset.cpp
  ${BASE}/table_reader.cpp
  ${BASE}/transaction.cpp
)
set( HDRS
  ${BASE}/admin.h
  ${BASE}/box.h
  ${BASE}/client.h
  ${BASE}/config.h
  ${BASE}/connection.h
  ${BASE}/file_reader.h
  ${BASE}/file_writer.h
  ${BASE}/journal_reader.h
  ${BASE}/journal_writer.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/rowset.h
  ${BASE}/table_reader.h
  ${BASE}/transaction.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
