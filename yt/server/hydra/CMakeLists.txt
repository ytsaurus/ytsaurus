#:yt-updatable
set( BASE ${CMAKE_SOURCE_DIR}/yt/server/hydra )
set( SRCS
  ${BASE}/private.cpp
  ${BASE}/public.cpp
  ${BASE}/persistent_response_keeper.cpp
  ${BASE}/file_helpers.cpp
  ${BASE}/sync_file_changelog.cpp
  ${BASE}/sync_file_changelog_impl.cpp
  ${BASE}/file_changelog_dispatcher.cpp
  ${BASE}/local_changelog_store.cpp
  ${BASE}/file_snapshot_store.cpp
  ${BASE}/mutation_context.cpp
  ${BASE}/decorated_automaton.cpp
  ${BASE}/serialize.cpp
  ${BASE}/snapshot_discovery.cpp
  ${BASE}/snapshot_download.cpp
  ${BASE}/changelog_download.cpp
  ${BASE}/changelog_discovery.cpp
  ${BASE}/recovery.cpp
  ${BASE}/follower_tracker.cpp
  ${BASE}/hydra_manager.cpp
  ${BASE}/changelog.cpp
  ${BASE}/snapshot.cpp
  ${BASE}/distributed_hydra_manager.cpp
  ${BASE}/mutation_committer.cpp
  ${BASE}/checkpointer.cpp
  ${BASE}/composite_automaton.cpp
  ${BASE}/mutation.cpp
  ${BASE}/hydra_service.cpp
  ${BASE}/rpc_helpers.cpp
  ${BASE}/local_snapshot_service.cpp
  ${BASE}/local_snapshot_store.cpp
  ${BASE}/remote_snapshot_store.cpp
  ${BASE}/remote_changelog_store.cpp
  ${BASE}/lazy_changelog.cpp
)
set( HDRS
  ${BASE}/private.h
  ${BASE}/public.h
  ${BASE}/persistent_response_keeper.h
  ${BASE}/file_helpers.h
  ${BASE}/file_helpers-inl.h
  ${BASE}/changelog.h
  ${BASE}/sync_file_changelog.h
  ${BASE}/sync_file_changelog_impl.h
  ${BASE}/file_changelog_dispatcher.h
  ${BASE}/local_changelog_store.h
  ${BASE}/config.h
  ${BASE}/snapshot.h
  ${BASE}/file_snapshot_store.h
  ${BASE}/automaton.h
  ${BASE}/mutation_context.h
  ${BASE}/decorated_automaton.h
  ${BASE}/serialize.h
  ${BASE}/snapshot_discovery.h
  ${BASE}/snapshot_download.h
  ${BASE}/changelog_download.h
  ${BASE}/changelog_discovery.h
  ${BASE}/recovery.h
  ${BASE}/follower_tracker.h
  ${BASE}/hydra_manager.h
  ${BASE}/distributed_hydra_manager.h
  ${BASE}/mutation_committer.h
  ${BASE}/checkpointer.h
  ${BASE}/composite_automaton.h
  ${BASE}/composite_automaton-inl.h
  ${BASE}/mutation.h
  ${BASE}/mutation-inl.h
  ${BASE}/entity_map-inl.h
  ${BASE}/entity_map.h
  ${BASE}/hydra_service.h
  ${BASE}/rpc_helpers.h
  ${BASE}/snapshot_service_proxy.h
  ${BASE}/local_snapshot_service.h
  ${BASE}/local_snapshot_store.h
  ${BASE}/remote_changelog_store.h
  ${BASE}/remote_snapshot_store.h
  ${BASE}/lazy_changelog.h
)

list(APPEND YT_SOURCE_FILES
  ${SRCS}
  ${HDRS}
)

protoc( ${BASE}/snapshot_service.proto YT_SOURCE_FILES )
protoc( ${BASE}/persistent_response_keeper.proto YT_SOURCE_FILES )
