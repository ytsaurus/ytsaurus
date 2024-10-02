#pragma once

#include "public.h"

#include <yt/yt/core/rpc/client.h>

////////////////////////////////////////////////////////////////////////////////

namespace google::protobuf {
    class EnumDescriptor;
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT::NOrm::NClient::NNative {

////////////////////////////////////////////////////////////////////////////////

TString ConvertCppToGrpcNamespace(TStringBuf cppNamespace);

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_OBJECT_SERVICE_PROXY_BY_NAMESPACE(ns) \
    DEFINE_OBJECT_SERVICE_PROXY_BY_NAMESPACES(ns, ns)

#define DEFINE_OBJECT_SERVICE_PROXY_BY_NAMESPACES(objectNs, apiNs) \
    class TObjectServiceProxy \
        : public NYT::NRpc::TProxyBase \
    { \
    public: \
        using EObjectType = objectNs::EObjectType; \
        using ESelectObjectHistoryIndexMode = apiNs::ESelectObjectHistoryIndexMode; \
        \
        static const google::protobuf::EnumDescriptor* GetObjectTypeEnumDescriptor() { \
            return objectNs::EObjectType_descriptor(); \
        } \
        \
        DEFINE_RPC_PROXY(TObjectServiceProxy, ObjectService, \
            .SetNamespace(NYT::NOrm::NClient::NNative::ConvertCppToGrpcNamespace(#apiNs))); \
        \
        DEFINE_RPC_PROXY_METHOD(apiNs, GenerateTimestamp); \
        DEFINE_RPC_PROXY_METHOD(apiNs, StartTransaction); \
        DEFINE_RPC_PROXY_METHOD(apiNs, CommitTransaction); \
        DEFINE_RPC_PROXY_METHOD(apiNs, AbortTransaction); \
        DEFINE_RPC_PROXY_METHOD(apiNs, CreateObject); \
        DEFINE_RPC_PROXY_METHOD(apiNs, CreateObjects); \
        DEFINE_RPC_PROXY_METHOD(apiNs, RemoveObject); \
        DEFINE_RPC_PROXY_METHOD(apiNs, RemoveObjects); \
        DEFINE_RPC_PROXY_METHOD(apiNs, UpdateObject); \
        DEFINE_RPC_PROXY_METHOD(apiNs, UpdateObjects); \
        DEFINE_RPC_PROXY_METHOD(apiNs, GetObject); \
        DEFINE_RPC_PROXY_METHOD(apiNs, GetObjects); \
        DEFINE_RPC_PROXY_METHOD(apiNs, SelectObjects); \
        DEFINE_RPC_PROXY_METHOD(apiNs, CheckObjectPermissions); \
        DEFINE_RPC_PROXY_METHOD(apiNs, GetObjectAccessAllowedFor); \
        DEFINE_RPC_PROXY_METHOD(apiNs, GetUserAccessAllowedTo); \
        DEFINE_RPC_PROXY_METHOD(apiNs, WatchObjects); \
        DEFINE_RPC_PROXY_METHOD(apiNs, SelectObjectHistory); \
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative
