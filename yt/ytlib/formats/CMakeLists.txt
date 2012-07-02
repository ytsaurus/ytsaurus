#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/formats )
set( SRCS
  ${BASE}/dsv_parser.cpp
  ${BASE}/dsv_writer.cpp
  ${BASE}/format.cpp
  ${BASE}/json_parser.cpp
  ${BASE}/json_writer.cpp
  ${BASE}/yamr_parser.cpp
  ${BASE}/yamr_writer.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/dsv_parser.h
  ${BASE}/dsv_writer.h
  ${BASE}/format.h
  ${BASE}/json_parser.h
  ${BASE}/json_writer.h
  ${BASE}/public.h
  ${BASE}/yamr_parser.h
  ${BASE}/yamr_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
