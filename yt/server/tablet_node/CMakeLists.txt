#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/tablet_node )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/tablet_slot.cpp
  ${BASE}/tablet_cell_controller.cpp
  ${BASE}/automaton.cpp
  ${BASE}/tablet_manager.cpp
  ${BASE}/tablet.cpp
  ${BASE}/serialize.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/tablet_slot.h
  ${BASE}/tablet_cell_controller.h
  ${BASE}/automaton.h
  ${BASE}/tablet_manager.h
  ${BASE}/tablet.h
  ${BASE}/serialize.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_manager.proto YT_SOURCE_FILES )
