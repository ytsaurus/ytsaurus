#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
  ${BASE}/load_context.cpp
  ${BASE}/meta_state_facade.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/load_context-inl.h
  ${BASE}/load_context.h
  ${BASE}/public.h
  ${BASE}/meta_state_facade.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)