#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/cell_master )
set( SRCS
  ${BASE}/automaton.cpp
  ${BASE}/bootstrap.cpp
  ${BASE}/config.cpp
  ${BASE}/hydra_facade.cpp
  ${BASE}/master_hydra_service.cpp
  ${BASE}/private.cpp
  ${BASE}/serialize.cpp
  ${BASE}/world_initializer.cpp
  ${BASE}/multicell_manager.cpp
)
set( HDRS
  ${BASE}/automaton.h
  ${BASE}/bootstrap.h
  ${BASE}/config.h
  ${BASE}/hydra_facade.h
  ${BASE}/master_hydra_service.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/serialize-inl.h
  ${BASE}/serialize.h
  ${BASE}/world_initializer.h
  ${BASE}/multicell_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/multicell_manager.proto YT_SOURCE_FILES )
