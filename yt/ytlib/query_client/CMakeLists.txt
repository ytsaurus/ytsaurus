#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client )
set( SRCS
  ${BASE}/ast_visitor.cpp
  ${BASE}/graphviz.cpp
  ${BASE}/operator.cpp
  ${BASE}/expression.cpp
  ${BASE}/query_context.cpp
  ${BASE}/query_fragment.cpp
  ${BASE}/private.cpp
  ${BASE}/helpers.cpp
  ${BASE}/coordinate_controller.cpp
  ${BASE}/evaluate_controller.cpp
  ${BASE}/executor.cpp
)

set( HDRS
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

bison( ${BASE}/parser.yy YT_SOURCE_FILES )
ragel( ${BASE}/lexer.rl YT_SOURCE_FILES )

protoc( ${BASE}/operator.proto YT_SOURCE_FILES )
protoc( ${BASE}/expression.proto YT_SOURCE_FILES )
protoc( ${BASE}/query_fragment.proto YT_SOURCE_FILES )
protoc( ${BASE}/query_service.proto YT_SOURCE_FILES )

