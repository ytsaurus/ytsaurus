#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/tablet_node )
set( SRCS
  ${BASE}/automaton.cpp
  ${BASE}/chunk_store.cpp
  ${BASE}/dynamic_memory_store.cpp
  ${BASE}/dynamic_memory_store_bits.cpp
  ${BASE}/dynamic_memory_store_comparer.cpp
  ${BASE}/lookup.cpp
  ${BASE}/partition.cpp
  ${BASE}/partition_balancer.cpp
  ${BASE}/private.cpp
  ${BASE}/row_comparer_generator.cpp
  ${BASE}/row_merger.cpp
  ${BASE}/security_manager.cpp
  ${BASE}/serialize.cpp
  ${BASE}/slot_manager.cpp
  ${BASE}/store.cpp
  ${BASE}/store_compactor.cpp
  ${BASE}/store_detail.cpp
  ${BASE}/store_flusher.cpp
  ${BASE}/store_manager.cpp
  ${BASE}/store_preloader.cpp
  ${BASE}/tablet.cpp
  ${BASE}/tablet_manager.cpp
  ${BASE}/tablet_reader.cpp
  ${BASE}/tablet_service.cpp
  ${BASE}/tablet_slot.cpp
  ${BASE}/transaction.cpp
  ${BASE}/transaction_manager.cpp
)
set( HDRS
  ${BASE}/automaton.h
  ${BASE}/chunk_store.h
  ${BASE}/config.h
  ${BASE}/dynamic_memory_store.h
  ${BASE}/dynamic_memory_store_bits.h
  ${BASE}/dynamic_memory_store_comparer.h
  ${BASE}/llvm_types.h
  ${BASE}/lookup.h
  ${BASE}/partition.h
  ${BASE}/partition_balancer.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/row_comparer_generator.h
  ${BASE}/row_merger.h
  ${BASE}/security_manager.h
  ${BASE}/serialize.h
  ${BASE}/slot_manager.h
  ${BASE}/store.h
  ${BASE}/store_compactor.h
  ${BASE}/store_detail.h
  ${BASE}/store_flusher.h
  ${BASE}/store_manager.h
  ${BASE}/store_preloader.h
  ${BASE}/tablet.h
  ${BASE}/tablet_manager.h
  ${BASE}/tablet_reader.h
  ${BASE}/tablet_service.h
  ${BASE}/tablet_slot.h
  ${BASE}/transaction.h
  ${BASE}/transaction_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/tablet_manager.proto YT_SOURCE_FILES )
