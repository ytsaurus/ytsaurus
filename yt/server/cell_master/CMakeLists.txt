#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
  ${BASE}/automaton.cpp
  ${BASE}/meta_state_facade.cpp
  ${BASE}/hydra_service.cpp
  ${BASE}/serialization_context.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/automaton.h
  ${BASE}/config.h
  ${BASE}/meta_state_facade-inl.h
  ${BASE}/meta_state_facade.h
  ${BASE}/hydra_service.h
  ${BASE}/public.h
  ${BASE}/serialization_context-inl.h
  ${BASE}/serialization_context.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)