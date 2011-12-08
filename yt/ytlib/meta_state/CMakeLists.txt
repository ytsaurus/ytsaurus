SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_state.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/decorated_meta_state.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_state_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_state_manager_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/transient_state_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/persistent_state_manager.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/snapshot.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/change_log.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/change_committer.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/async_change_log.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/change_log_downloader.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/change_log_cache.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/snapshot_creator.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/snapshot_downloader.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/snapshot_store.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/recovery.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/follower_tracker.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/cell_manager-inl.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/cell_manager.cpp
   
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/follower_pinger.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/map.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/map-inl.h
    
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/composite_meta_state-inl.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/composite_meta_state.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/composite_meta_state_detail.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/composite_meta_state_detail-inl.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_change.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_change-inl.h
    
    ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_state_service.h
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_state_manager.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/meta_state/meta_state_manager_rpc.proto YT_SOURCE_FILES )
