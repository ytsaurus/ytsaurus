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
  ${BASE}/coordinator.cpp
)
set( HDRS
  ${BASE}/ast.h
  ${BASE}/ast_visitor.h
  ${BASE}/ast_visitor.h
  ${BASE}/coordinator.h
  ${BASE}/expression.h
  ${BASE}/graphviz.h
  ${BASE}/lexer.h
  ${BASE}/operator.h
  ${BASE}/parser.hpp
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/query_context.h
  ${BASE}/query_fragment.h
  ${BASE}/query_service_proxy.h
  ${BASE}/stubs.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

bison( ${BASE}/parser.yy YT_SOURCE_FILES )
ragel( ${BASE}/lexer.rl YT_SOURCE_FILES )

protoc( ${BASE}/operator.proto YT_SOURCE_FILES )
protoc( ${BASE}/expression.proto YT_SOURCE_FILES )
protoc( ${BASE}/query_service.proto YT_SOURCE_FILES )
