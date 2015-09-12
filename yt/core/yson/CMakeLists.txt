#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/async_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lexer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/parser.cpp
  ${CMAKE_CURRENT_LIST_DIR}/token.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tokenizer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/async_writer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/string.cpp
  ${CMAKE_CURRENT_LIST_DIR}/stream.cpp
  ${CMAKE_CURRENT_LIST_DIR}/producer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/forwarding_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/null_consumer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/attribute_consumer.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/async_consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/detail.h
  ${CMAKE_CURRENT_LIST_DIR}/format.h
  ${CMAKE_CURRENT_LIST_DIR}/lexer.h
  ${CMAKE_CURRENT_LIST_DIR}/lexer_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/parser.h
  ${CMAKE_CURRENT_LIST_DIR}/parser_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/token.h
  ${CMAKE_CURRENT_LIST_DIR}/tokenizer.h
  ${CMAKE_CURRENT_LIST_DIR}/writer.h
  ${CMAKE_CURRENT_LIST_DIR}/async_writer.h
  ${CMAKE_CURRENT_LIST_DIR}/string.h
  ${CMAKE_CURRENT_LIST_DIR}/stream.h
  ${CMAKE_CURRENT_LIST_DIR}/producer.h
  ${CMAKE_CURRENT_LIST_DIR}/forwarding_consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/null_consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/async_consumer.h
  ${CMAKE_CURRENT_LIST_DIR}/attribute_consumer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
