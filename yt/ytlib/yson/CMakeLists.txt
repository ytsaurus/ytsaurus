#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/yson )
set( SRCS
  ${BASE}/lexer.cpp
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
  ${BASE}/varint.cpp
  ${BASE}/consumer.cpp
  ${BASE}/parser.cpp
  ${BASE}/writer.cpp
)
set( HDRS
  ${BASE}/detail.h
  ${BASE}/lexer_detail.h
  ${BASE}/parser_detail.h
  ${BASE}/lexer.h
  ${BASE}/public.h
  ${BASE}/token.h
  ${BASE}/tokenizer.h
  ${BASE}/varint.h
  ${BASE}/consumer.h
  ${BASE}/format.h
  ${BASE}/parser.h
  ${BASE}/writer.h
  ${BASE}/zigzag.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
