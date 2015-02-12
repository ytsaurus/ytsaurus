#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client )
set( SRCS
  ${BASE}/plan_helpers.cpp
  ${BASE}/private.cpp
  ${BASE}/helpers.cpp
  ${BASE}/coordinator.cpp
  ${BASE}/key_trie.cpp
  ${BASE}/ast.cpp
  ${BASE}/plan_fragment.cpp
  ${BASE}/plan_fragment_common.cpp
  ${BASE}/query_statistics.cpp  
  ${BASE}/evaluator.cpp
  ${BASE}/evaluation_helpers.cpp
)

set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/helpers.h
  ${BASE}/callbacks.h
  ${BASE}/config.h
  ${BASE}/plan_helpers.h
  ${BASE}/coordinator.h
  ${BASE}/lexer.h
  ${BASE}/parser.hpp
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/query_service_proxy.h
  ${BASE}/key_trie.h
  ${BASE}/ast.h
  ${BASE}/plan_fragment.h
  ${BASE}/plan_fragment_common.h
  ${BASE}/query_statistics.h
  ${BASE}/evaluator.h
  ${BASE}/evaluation_helpers.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
    ${BASE}/cg_routines.cpp
    ${BASE}/cg_ir_builder.cpp
    ${BASE}/cg_fragment_compiler.cpp
    ${BASE}/folding_profiler.cpp
  )

  set( HDRS
    ${HDRS}
    ${BASE}/cg_types.h
    ${BASE}/cg_routines.h
    ${BASE}/cg_ir_builder.h
    ${BASE}/cg_fragment_compiler.h
    ${BASE}/folding_profiler.h
  )
endif()

list( APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

bison( ${BASE}/parser.yy YT_SOURCE_FILES )
ragel( ${BASE}/lexer.rl YT_SOURCE_FILES )

protoc( ${BASE}/query_statistics.proto YT_SOURCE_FILES )
protoc( ${BASE}/plan_fragment.proto YT_SOURCE_FILES )
protoc( ${BASE}/query_service.proto YT_SOURCE_FILES )

