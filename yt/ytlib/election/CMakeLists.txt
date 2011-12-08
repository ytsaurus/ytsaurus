SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/election/election_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/election/election_manager_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/election/leader_lookup.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/election/common.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/election/cell_channel.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/election/election_manager_rpc.proto YT_SOURCE_FILES )
