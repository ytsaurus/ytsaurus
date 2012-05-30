#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/formats )
set( SRCS
  ${BASE}/format.cpp
  ${BASE}/tsv_parser.cpp
  ${BASE}/tsv_writer.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/format.h
  ${BASE}/public.h
  ${BASE}/tsv_parser.h
  ${BASE}/tsv_writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
