#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
  ${BASE}/load_context.cpp
  ${BASE}/meta_state_service.cpp
  ${BASE}/world_initializer.cpp
)
set( HDRS
  ${BASE}/public.h
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/load_context-inl.h
  ${BASE}/load_context.h
  ${BASE}/meta_state_service.h
  ${BASE}/world_initializer.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)