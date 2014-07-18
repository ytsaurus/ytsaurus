#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client )
set( SRCS
  ${BASE}/plan_visitor.cpp
  ${BASE}/plan_helpers.cpp
  ${BASE}/operator.cpp
  ${BASE}/expression.cpp
  ${BASE}/plan_context.cpp
  ${BASE}/plan_fragment.cpp
  ${BASE}/private.cpp
  ${BASE}/helpers.cpp
  ${BASE}/coordinator.cpp
  ${BASE}/prepare_controller.cpp
  ${BASE}/executor.cpp
  ${BASE}/key_trie.cpp
)

set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/helpers.h
  ${BASE}/callbacks.h
  ${BASE}/plan_node.h
  ${BASE}/plan_visitor.h
  ${BASE}/plan_helpers.h
  ${BASE}/coordinator.h
  ${BASE}/prepare_controller.h
  ${BASE}/executor.h
  ${BASE}/expression.h
  ${BASE}/lexer.h
  ${BASE}/operator.h
  ${BASE}/parser.hpp
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/plan_context.h
  ${BASE}/plan_fragment.h
  ${BASE}/query_service_proxy.h
  ${BASE}/key_trie.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
    ${BASE}/evaluator.cpp
    ${BASE}/cg_routine_registry.cpp
    ${BASE}/cg_routines.cpp
    ${BASE}/cg_ir_builder.cpp
    ${BASE}/cg_fragment.cpp
    ${BASE}/cg_fragment_compiler.cpp
  )

  set( HDRS
    ${HDRS}
    ${BASE}/evaluator.h
    ${BASE}/cg_types.h
    ${BASE}/cg_routine_registry.h
    ${BASE}/cg_routines.h
    ${BASE}/cg_ir_builder.h
    ${BASE}/cg_fragment.h
    ${BASE}/cg_fragment_compiler.h
  )
endif()

list( APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

bison( ${BASE}/parser.yy YT_SOURCE_FILES )
ragel( ${BASE}/lexer.rl YT_SOURCE_FILES )

protoc( ${BASE}/operator.proto YT_SOURCE_FILES )
protoc( ${BASE}/expression.proto YT_SOURCE_FILES )
protoc( ${BASE}/plan_fragment.proto YT_SOURCE_FILES )
protoc( ${BASE}/query_service.proto YT_SOURCE_FILES )

