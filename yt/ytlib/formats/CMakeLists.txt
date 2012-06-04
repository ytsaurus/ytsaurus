#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/formats )
set( SRCS
  ${BASE}/format.cpp
  ${BASE}/dsv_parser.cpp
  ${BASE}/dsv_writer.cpp
  ${BASE}/json_writer.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/format.h
  ${BASE}/public.h
  ${BASE}/dsv_parser.h
  ${BASE}/dsv_writer.h
  ${BASE}/json_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
