#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/tablet_node )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/tablet_slot.cpp
  ${BASE}/tablet_slot_manager.cpp
  ${BASE}/automaton.cpp
  ${BASE}/tablet_manager.cpp
  ${BASE}/tablet.cpp
  ${BASE}/partition.cpp
  ${BASE}/serialize.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
  ${BASE}/tablet_service.cpp
  ${BASE}/store.cpp
  ${BASE}/store_detail.cpp
  ${BASE}/dynamic_memory_store.cpp
  ${BASE}/dynamic_memory_store_bits.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/store_manager.cpp
  ${BASE}/store_flusher.cpp
  ${BASE}/store_compactor.cpp
  ${BASE}/partition_balancer.cpp
  ${BASE}/tablet_reader.cpp
  ${BASE}/row_merger.cpp
  ${BASE}/lookup.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/config.h
  ${BASE}/tablet_slot.h
  ${BASE}/tablet_slot_manager.h
  ${BASE}/automaton.h
  ${BASE}/tablet_manager.h
  ${BASE}/tablet.h
  ${BASE}/partition.h
  ${BASE}/serialize.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
  ${BASE}/tablet_service.h
  ${BASE}/store.h
  ${BASE}/store_detail.h
  ${BASE}/dynamic_memory_store.h
  ${BASE}/dynamic_memory_store_bits.h
  ${BASE}/chunk_store.h
  ${BASE}/store_manager.h
  ${BASE}/store_flusher.h
  ${BASE}/store_compactor.h
  ${BASE}/partition_balancer.h
  ${BASE}/tablet_reader.h
  ${BASE}/row_merger.h
  ${BASE}/lookup.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_manager.proto YT_SOURCE_FILES )
