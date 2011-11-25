SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid/orchid_service.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid/orchid_service_rpc.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid/cypress_integration.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/orchid/orchid_service_rpc.proto YT_SOURCE_FILES )
