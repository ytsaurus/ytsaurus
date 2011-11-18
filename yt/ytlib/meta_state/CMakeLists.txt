SET(YT_SOURCE_FILES
    common.cpp

    meta_state.h

    decorated_meta_state.cpp

    meta_state_manager.cpp
    meta_state_manager.proto
    meta_state_manager_rpc.proto
    meta_state_manager_rpc.h

    snapshot.cpp

    change_log.cpp

    change_committer.cpp

    async_change_log.cpp

    change_log_downloader.cpp

    change_log_cache.cpp

    snapshot_creator.cpp

    snapshot_downloader.cpp

    snapshot_store.cpp

    recovery.cpp

    follower_tracker.cpp

    cell_manager-inl.h
    cell_manager.cpp
   
    cell_channel.cpp

    follower_pinger.cpp

    map.h
    map-inl.h
    
    composite_meta_state-inl.h
    composite_meta_state.cpp

    composite_meta_state_detail.cpp
    composite_meta_state_detail-inl.h

    meta_change.h
    meta_change-inl.h
    
    meta_state_service.h
)
