#:yt-updatable
set( SRCS
  ${CMAKE_CURRENT_LIST_DIR}/automaton.cpp
  ${CMAKE_CURRENT_LIST_DIR}/chunk_store.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store_bits.cpp
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store_comparer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/lookup.cpp
  ${CMAKE_CURRENT_LIST_DIR}/partition.cpp
  ${CMAKE_CURRENT_LIST_DIR}/partition_balancer.cpp
  ${CMAKE_CURRENT_LIST_DIR}/private.cpp
  ${CMAKE_CURRENT_LIST_DIR}/row_comparer_generator.cpp
  ${CMAKE_CURRENT_LIST_DIR}/security_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/serialize.cpp
  ${CMAKE_CURRENT_LIST_DIR}/slot_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/store.cpp
  ${CMAKE_CURRENT_LIST_DIR}/store_compactor.cpp
  ${CMAKE_CURRENT_LIST_DIR}/store_detail.cpp
  ${CMAKE_CURRENT_LIST_DIR}/store_flusher.cpp
  ${CMAKE_CURRENT_LIST_DIR}/store_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_reader.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_service.cpp
  ${CMAKE_CURRENT_LIST_DIR}/tablet_slot.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction.cpp
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.cpp
  ${CMAKE_CURRENT_LIST_DIR}/in_memory_manager.cpp
)
set( HDRS
  ${CMAKE_CURRENT_LIST_DIR}/automaton.h
  ${CMAKE_CURRENT_LIST_DIR}/chunk_store.h
  ${CMAKE_CURRENT_LIST_DIR}/config.h
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store.h
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store_bits.h
  ${CMAKE_CURRENT_LIST_DIR}/dynamic_memory_store_comparer.h
  ${CMAKE_CURRENT_LIST_DIR}/llvm_types.h
  ${CMAKE_CURRENT_LIST_DIR}/lookup.h
  ${CMAKE_CURRENT_LIST_DIR}/partition.h
  ${CMAKE_CURRENT_LIST_DIR}/partition_balancer.h
  ${CMAKE_CURRENT_LIST_DIR}/private.h
  ${CMAKE_CURRENT_LIST_DIR}/public.h
  ${CMAKE_CURRENT_LIST_DIR}/row_comparer_generator.h
  ${CMAKE_CURRENT_LIST_DIR}/security_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/serialize.h
  ${CMAKE_CURRENT_LIST_DIR}/slot_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/store.h
  ${CMAKE_CURRENT_LIST_DIR}/store_compactor.h
  ${CMAKE_CURRENT_LIST_DIR}/store_detail.h
  ${CMAKE_CURRENT_LIST_DIR}/store_flusher.h
  ${CMAKE_CURRENT_LIST_DIR}/store_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_reader.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_service.h
  ${CMAKE_CURRENT_LIST_DIR}/tablet_slot.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction.h
  ${CMAKE_CURRENT_LIST_DIR}/transaction_manager.h
  ${CMAKE_CURRENT_LIST_DIR}/in_memory_manager.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${CMAKE_CURRENT_LIST_DIR}/tablet_manager.proto YT_SOURCE_FILES )
