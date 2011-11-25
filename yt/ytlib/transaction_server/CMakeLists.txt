SET( YT_SOURCE_FILES ${YT_SOURCE_FILES}
    ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/common.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/transaction.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/transaction_service_rpc.h
    ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/transaction_service.cpp

    ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/transaction_manager.cpp
    ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/cypress_integration.cpp
)

PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/transaction_service_rpc.proto YT_SOURCE_FILES )
PROTOC( ${CMAKE_SOURCE_DIR}/yt/ytlib/transaction_server/transaction_manager.proto YT_SOURCE_FILES )
