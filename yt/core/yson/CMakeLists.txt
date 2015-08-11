#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/yson )
set( SRCS
  ${BASE}/consumer.cpp
  ${BASE}/async_consumer.cpp
  ${BASE}/lexer.cpp
  ${BASE}/parser.cpp
  ${BASE}/token.cpp
  ${BASE}/tokenizer.cpp
  ${BASE}/writer.cpp
  ${BASE}/async_writer.cpp
  ${BASE}/string.cpp
  ${BASE}/stream.cpp
  ${BASE}/producer.cpp
  ${BASE}/forwarding_consumer.cpp
  ${BASE}/null_consumer.cpp
  ${BASE}/attribute_consumer.cpp
)
set( HDRS
  ${BASE}/consumer.h
  ${BASE}/async_consumer.h
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
  ${BASE}/async_writer.h
  ${BASE}/string.h
  ${BASE}/stream.h
  ${BASE}/producer.h
  ${BASE}/forwarding_consumer.h
  ${BASE}/null_consumer.h
  ${BASE}/async_consumer.h
  ${BASE}/attribute_consumer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
