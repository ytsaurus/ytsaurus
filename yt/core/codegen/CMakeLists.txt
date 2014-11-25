#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/core/codegen )
set( SRCS
)
set( HDRS
  ${BASE}/public.h
)

if (YT_BUILD_ENABLE_LLVM)
  set( SRCS
    ${SRCS}
    ${BASE}/private.cpp
    ${BASE}/init.cpp
    ${BASE}/module.cpp
    ${BASE}/routine_registry.cpp
  )
  set( HDRS
    ${HDRS}
    ${BASE}/private.h
    ${BASE}/init.h
    ${BASE}/module.h
    ${BASE}/module-inl.h
    ${BASE}/routine_registry.h
  )
endif()


list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

