SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/node.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/node_detail.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/node_proxy.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/node_proxy_detail.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/lock.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_manager.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_service.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_service_rpc.h

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/virtual.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_integration.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_ypath_rpc.h
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_ypath_rpc.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_service_rpc.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/cypress/cypress_manager.proto YT_SOURCE_FILES )
