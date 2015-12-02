set( BASE ${CMAKE_SOURCE_DIR}/yt/core/codegen )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/init.cpp
  ${BASE}/module.cpp
  ${BASE}/routine_registry.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/private.h
  ${BASE}/init.h
  ${BASE}/function.h
  ${BASE}/module.h
  ${BASE}/module-inl.h
  ${BASE}/routine_registry.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

