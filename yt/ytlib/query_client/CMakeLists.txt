#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/query_client )
set( SRCS
  ${BASE}/plan_visitor.cpp
  ${BASE}/plan_helpers.cpp
  ${BASE}/graphviz.cpp
  ${BASE}/operator.cpp
  ${BASE}/expression.cpp
  ${BASE}/plan_context.cpp
  ${BASE}/plan_fragment.cpp
  ${BASE}/private.cpp
  ${BASE}/helpers.cpp
  ${BASE}/coordinate_controller.cpp
  ${BASE}/prepare_controller.cpp
  ${BASE}/executor.cpp
  ${BASE}/source_location.cpp
)

set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/helpers.h
  ${BASE}/callbacks.h
  ${BASE}/plan_node.h
  ${BASE}/plan_visitor.h
  ${BASE}/plan_helpers.h
  ${BASE}/coordinate_controller.h
  ${BASE}/prepare_controller.h
  ${BASE}/executor.h
  ${BASE}/expression.h
  ${BASE}/graphviz.h
  ${BASE}/lexer.h
  ${BASE}/operator.h
  ${BASE}/parser.hpp
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/plan_context.h
  ${BASE}/plan_fragment.h
  ${BASE}/query_service_proxy.h
  ${BASE}/source_location.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
    ${BASE}/codegen_controller.cpp
  )

  set( HDRS
    ${HDRS}
    ${BASE}/codegen_controller.h
  )

#  set_source_files_properties(
#    ${BASE}/codegen_controller.cpp
#    PROPERTIES
#    COMPILE_FLAGS ${LLVM_CPPFLAGS}
#  )
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

