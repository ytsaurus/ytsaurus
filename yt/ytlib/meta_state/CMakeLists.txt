#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state )
set( SRCS
  ${BASE}/mutation_context.cpp
  ${BASE}/async_change_log.cpp
  ${BASE}/mutation_committer.cpp
  ${BASE}/change_log.cpp
  ${BASE}/change_log_cache.cpp
  ${BASE}/change_log_downloader.cpp
  ${BASE}/change_log_impl.cpp
  ${BASE}/private.cpp
  ${BASE}/composite_meta_state.cpp
  ${BASE}/serialize.cpp
  ${BASE}/decorated_meta_state.cpp
  ${BASE}/file_helpers.cpp
  ${BASE}/follower_pinger.cpp
  ${BASE}/follower_tracker.cpp
  ${BASE}/meta_state_manager.cpp
  ${BASE}/persistent_state_manager.cpp
  ${BASE}/recovery.cpp
  ${BASE}/snapshot.cpp
  ${BASE}/snapshot_builder.cpp
  ${BASE}/snapshot_downloader.cpp
  ${BASE}/snapshot_lookup.cpp
  ${BASE}/snapshot_store.cpp
)
set( HDRS
  ${BASE}/mutation_context.h
  ${BASE}/async_change_log.h
  ${BASE}/mutation_committer.h
  ${BASE}/change_log.h
  ${BASE}/change_log_cache.h
  ${BASE}/change_log_downloader.h
  ${BASE}/change_log_impl.h
  ${BASE}/private.h
  ${BASE}/composite_meta_state-inl.h
  ${BASE}/composite_meta_state.h
  ${BASE}/serialize.h
  ${BASE}/config.h
  ${BASE}/decorated_meta_state.h
  ${BASE}/file_helpers.h
  ${BASE}/follower_pinger.h
  ${BASE}/follower_tracker.h
  ${BASE}/map-inl.h
  ${BASE}/map.h
  ${BASE}/mutation-inl.h
  ${BASE}/mutation.h
  ${BASE}/meta_state.h
  ${BASE}/meta_state_manager.h
  ${BASE}/meta_state_manager_proxy.h
  ${BASE}/meta_version.h
  ${BASE}/persistent_state_manager.h
  ${BASE}/public.h
  ${BASE}/recovery.h
  ${BASE}/snapshot.h
  ${BASE}/snapshot_builder.h
  ${BASE}/snapshot_downloader.h
  ${BASE}/snapshot_lookup.h
  ${BASE}/snapshot_store.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/meta_state_manager.proto YT_SOURCE_FILES )
