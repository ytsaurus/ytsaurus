#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
  ${BASE}/load_context.cpp
  ${BASE}/meta_state_facade.cpp
  ${BASE}/meta_state_service.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/load_context-inl.h
  ${BASE}/load_context.h
  ${BASE}/meta_state_facade-inl.h
  ${BASE}/meta_state_facade.h
  ${BASE}/meta_state_service.h
  ${BASE}/public.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)