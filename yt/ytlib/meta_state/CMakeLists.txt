#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state )
set( SRCS
  ${BASE}/public.cpp
  ${BASE}/async_change_log.cpp
  ${BASE}/change_log.cpp
  ${BASE}/change_log_cache.cpp
  ${BASE}/change_log_downloader.cpp
  ${BASE}/change_log_impl.cpp
  ${BASE}/composite_meta_state.cpp
  ${BASE}/decorated_meta_state.cpp
  ${BASE}/file_helpers.cpp
  ${BASE}/follower_pinger.cpp
  ${BASE}/quorum_tracker.cpp
  ${BASE}/master_channel.cpp
  ${BASE}/master_discovery.cpp
  ${BASE}/meta_state_manager.cpp
  ${BASE}/mutation.cpp
  ${BASE}/mutation_committer.cpp
  ${BASE}/mutation_context.cpp
  ${BASE}/persistent_state_manager.cpp
  ${BASE}/private.cpp
  ${BASE}/recovery.cpp
  ${BASE}/serialize.cpp
  ${BASE}/snapshot.cpp
  ${BASE}/snapshot_builder.cpp
  ${BASE}/snapshot_downloader.cpp
  ${BASE}/snapshot_lookup.cpp
  ${BASE}/snapshot_store.cpp
  ${BASE}/rpc_helpers.cpp
  ${BASE}/response_keeper.cpp
)
set( HDRS
  ${BASE}/async_change_log.h
  ${BASE}/change_log.h
  ${BASE}/change_log_cache.h
  ${BASE}/change_log_downloader.h
  ${BASE}/change_log_impl.h
  ${BASE}/composite_meta_state-inl.h
  ${BASE}/composite_meta_state.h
  ${BASE}/config.h
  ${BASE}/decorated_meta_state.h
  ${BASE}/file_helpers.h
  ${BASE}/follower_pinger.h
  ${BASE}/quorum_tracker.h
  ${BASE}/master_channel.h
  ${BASE}/map-inl.h
  ${BASE}/map.h
  ${BASE}/master_discovery.h
  ${BASE}/meta_state.h
  ${BASE}/meta_state_manager.h
  ${BASE}/meta_state_manager_proxy.h
  ${BASE}/meta_version.h
  ${BASE}/mutation.h
  ${BASE}/mutation-inl.h
  ${BASE}/mutation_committer.h
  ${BASE}/mutation_context.h
  ${BASE}/persistent_state_manager.h
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/recovery.h
  ${BASE}/serialize.h
  ${BASE}/snapshot.h
  ${BASE}/snapshot_builder.h
  ${BASE}/snapshot_downloader.h
  ${BASE}/snapshot_lookup.h
  ${BASE}/snapshot_store.h
  ${BASE}/rpc_helpers.h
  ${BASE}/response_keeper.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/meta_state_manager.proto YT_SOURCE_FILES )
