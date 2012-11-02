#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/yson )
set( SRCS
  ${BASE}/lexer.cpp
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
  ${BASE}/varint.cpp
  ${BASE}/yson_consumer.cpp
  ${BASE}/yson_parser.cpp
  ${BASE}/yson_writer.cpp
)
set( HDRS
  ${BASE}/lexer.h
  ${BASE}/public.h
  ${BASE}/token.h
  ${BASE}/tokenizer.h
  ${BASE}/varint.h
  ${BASE}/yson_consumer.h
  ${BASE}/yson_format.h
  ${BASE}/yson_parser.h
  ${BASE}/yson_writer.h
  ${BASE}/zigzag.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
