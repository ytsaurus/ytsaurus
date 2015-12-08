set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/init.cpp
  ${CMAKE_CURRENT_LIST_DIR}/module.cpp
  ${CMAKE_CURRENT_LIST_DIR}/routine_registry.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/init.h
  ${CMAKE_CURRENT_LIST_DIR}/function.h
  ${CMAKE_CURRENT_LIST_DIR}/module.h
  ${CMAKE_CURRENT_LIST_DIR}/module-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/routine_registry.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

