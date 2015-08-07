#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/formats )
set( SRCS
  ${BASE}/dsv_parser.cpp
  ${BASE}/dsv_writer.cpp
  ${BASE}/format.cpp
  ${BASE}/helpers.cpp
  ${BASE}/json_callbacks.cpp
  ${BASE}/json_parser.cpp
  ${BASE}/json_writer.cpp
  ${BASE}/parser.cpp
  ${BASE}/schemaful_dsv_parser.cpp
  ${BASE}/schemaful_dsv_writer.cpp
  ${BASE}/schemaful_writer.cpp
  ${BASE}/schemaless_writer_adapter.cpp
  ${BASE}/symbols.cpp
  ${BASE}/utf8_decoder.cpp
  ${BASE}/yamr_base_parser.cpp
  ${BASE}/yamr_parser.cpp
  ${BASE}/yamr_writer.cpp
  ${BASE}/yamred_dsv_parser.cpp
  ${BASE}/yamred_dsv_writer.cpp
  ${BASE}/yson_parser.cpp
)
set( HDRS
  ${BASE}/config.h
  ${BASE}/dsv_parser.h
  ${BASE}/dsv_table.h
  ${BASE}/dsv_writer.h
  ${BASE}/format.h
  ${BASE}/helpers.h
  ${BASE}/json_callbacks.h
  ${BASE}/json_parser.h
  ${BASE}/json_writer.h
  ${BASE}/parser.h
  ${BASE}/public.h
  ${BASE}/schemaful_dsv_parser.h
  ${BASE}/schemaful_dsv_table.h
  ${BASE}/schemaful_dsv_writer.h
  ${BASE}/schemaful_writer.h
  ${BASE}/schemaless_writer_adapter.h
  ${BASE}/symbols.h
  ${BASE}/utf8_decoder.h
  ${BASE}/yamr_base_parser.h
  ${BASE}/yamr_parser.h
  ${BASE}/yamr_table.h
  ${BASE}/yamr_writer.h
  ${BASE}/yamred_dsv_parser.h
  ${BASE}/yamred_dsv_writer.h
  ${BASE}/yson_parser.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
