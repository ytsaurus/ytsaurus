#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cell_master )
set( SRCS
  ${BASE}/bootstrap.cpp
  ${BASE}/automaton.cpp
  ${BASE}/hydra_facade.cpp
  ${BASE}/world_initializer.cpp
  ${BASE}/master_hydra_service.cpp
  ${BASE}/serialize.cpp
)
set( HDRS
  ${BASE}/bootstrap.h
  ${BASE}/automaton.h
  ${BASE}/config.h
  ${BASE}/hydra_facade.h
  ${BASE}/world_initializer.h
  ${BASE}/master_hydra_service.h
  ${BASE}/public.h
  ${BASE}/serialize-inl.h
  ${BASE}/serialize.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)
