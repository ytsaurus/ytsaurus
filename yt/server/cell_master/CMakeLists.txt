#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/automaton.cpp
  ${CMAKE_CURRENT_LIST_DIR}/bootstrap.cpp
  ${CMAKE_CURRENT_LIST_DIR}/config.cpp
  ${CMAKE_CURRENT_LIST_DIR}/hydra_facade.cpp
  ${CMAKE_CURRENT_LIST_DIR}/master_hydra_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/world_initializer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/multicell_manager.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/automaton.h
  ${CMAKE_CURRENT_LIST_DIR}/bootstrap.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/hydra_facade.h
  ${CMAKE_CURRENT_LIST_DIR}/master_hydra_service.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize-inl.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize.h
  ${CMAKE_CURRENT_LIST_DIR}/world_initializer.h
  ${CMAKE_CURRENT_LIST_DIR}/multicell_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/multicell_manager.proto YT_SOURCE_FILES )
