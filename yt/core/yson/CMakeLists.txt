#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/yson )
set( SRCS
  ${BASE}/consumer.cpp
  ${BASE}/lexer.cpp
  ${BASE}/parser.cpp
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
  ${BASE}/writer.cpp
)
set( HDRS
  ${BASE}/consumer.h
  ${BASE}/detail.h
  ${BASE}/format.h
  ${BASE}/lexer.h
  ${BASE}/lexer_detail.h
  ${BASE}/parser.h
  ${BASE}/parser_detail.h
  ${BASE}/public.h
  ${BASE}/token.h
  ${BASE}/tokenizer.h
  ${BASE}/writer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
