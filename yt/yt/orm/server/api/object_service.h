#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/orm/server/objects/object.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NOrm::NServer::NApi {

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_OBJECT_SERVICE_PROTO_MODULE_FROM_NAMESPACES(clientNs, serverNs) \
    DEFINE_OBJECT_SERVICE_PROTO_MODULE_FROM_NAMESPACES_SEPARATELY(clientNs, clientNs, serverNs)

#define DEFINE_OBJECT_SERVICE_PROTO_MODULE_FROM_NAMESPACES_SEPARATELY(objectNs, clientNs, serverNs) \
    struct TObjectServiceProtoModule \
    { \
        using TReqGenerateTimestamp = clientNs::TReqGenerateTimestamp; \
        using TRspGenerateTimestamp = clientNs::TRspGenerateTimestamp; \
        \
        using TReqStartTransaction = clientNs::TReqStartTransaction; \
        using TRspStartTransaction = clientNs::TRspStartTransaction; \
        \
        using TReqCommitTransaction = clientNs::TReqCommitTransaction; \
        using TRspCommitTransaction = clientNs::TRspCommitTransaction; \
        \
        using TReqAbortTransaction = clientNs::TReqAbortTransaction; \
        using TRspAbortTransaction = clientNs::TRspAbortTransaction; \
        \
        using TReqCreateObject = clientNs::TReqCreateObject; \
        using TRspCreateObject = clientNs::TRspCreateObject; \
        \
        using TReqCreateObjects = clientNs::TReqCreateObjects; \
        using TRspCreateObjects = clientNs::TRspCreateObjects; \
        \
        using TReqRemoveObject = clientNs::TReqRemoveObject; \
        using TRspRemoveObject = clientNs::TRspRemoveObject; \
        \
        using TReqRemoveObjects = clientNs::TReqRemoveObjects; \
        using TRspRemoveObjects = clientNs::TRspRemoveObjects; \
        \
        using TReqUpdateObject = clientNs::TReqUpdateObject; \
        using TRspUpdateObject = clientNs::TRspUpdateObject; \
        \
        using TReqUpdateObjects = clientNs::TReqUpdateObjects; \
        using TRspUpdateObjects = clientNs::TRspUpdateObjects; \
        \
        using TReqGetObject = clientNs::TReqGetObject; \
        using TRspGetObject = clientNs::TRspGetObject; \
        \
        using TReqGetObjects = clientNs::TReqGetObjects; \
        using TRspGetObjects = clientNs::TRspGetObjects; \
        \
        using TReqSelectObjects = clientNs::TReqSelectObjects; \
        using TRspSelectObjects = clientNs::TRspSelectObjects; \
        \
        using TReqAggregateObjects = clientNs::TReqAggregateObjects; \
        using TRspAggregateObjects = clientNs::TRspAggregateObjects; \
        \
        using TReqCheckObjectPermissions = clientNs::TReqCheckObjectPermissions; \
        using TRspCheckObjectPermissions = clientNs::TRspCheckObjectPermissions; \
        \
        using TReqGetObjectAccessAllowedFor = clientNs::TReqGetObjectAccessAllowedFor; \
        using TRspGetObjectAccessAllowedFor = clientNs::TRspGetObjectAccessAllowedFor; \
        \
        using TReqGetUserAccessAllowedTo = clientNs::TReqGetUserAccessAllowedTo; \
        using TRspGetUserAccessAllowedTo = clientNs::TRspGetUserAccessAllowedTo; \
        \
        using TReqWatchObjects = clientNs::TReqWatchObjects; \
        using TRspWatchObjects = clientNs::TRspWatchObjects; \
        \
        using TReqSelectObjectHistory = clientNs::TReqSelectObjectHistory; \
        using TRspSelectObjectHistory = clientNs::TRspSelectObjectHistory; \
        \
        using TEvent = clientNs::TEvent; \
        using EEventType = clientNs::EEventType; \
        static const ::google::protobuf::EnumDescriptor* EEventType_descriptor() \
        { \
            return clientNs::EEventType_descriptor(); \
        } \
        \
        using TAttributeList = clientNs::TAttributeList; \
        using TSetUpdate = clientNs::TSetUpdate; \
        using TSetRootUpdate = clientNs::TSetRootUpdate; \
        using TUpdateIfExisting = clientNs::TUpdateIfExisting;  \
        using TLockUpdate = clientNs::TLockUpdate; \
        using TMethodCall = clientNs::TMethodCall; \
        \
        using EAccessControlAction = objectNs::EAccessControlAction; \
        using EObjectType = objectNs::EObjectType; \
        \
        using TSelectObjectsOptions = clientNs::TSelectObjectsOptions; \
        using TSelectObjectsContinuationToken = serverNs::TSelectObjectsContinuationToken; \
        \
        using ESelectObjectHistoryIndexMode = clientNs::ESelectObjectHistoryIndexMode; \
        using TSelectObjectHistoryOptions = clientNs::TSelectObjectHistoryOptions; \
        using TSelectObjectHistoryContinuationToken = serverNs::TSelectObjectHistoryContinuationToken; \
    };

////////////////////////////////////////////////////////////////////////////////

struct TTrivialObjectTypeRemapper
{
    template <class T>
    static T Remap(T objectType)
    {
        return objectType;
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Object service proto module incorporates all required proto message types.
//! Object type remapper allows to implement object type migration.
template <
    class TObjectServiceProtoModule,
    class TObjectTypeRemapper = TTrivialObjectTypeRemapper>
NRpc::IServicePtr CreateObjectService(
    NMaster::IBootstrap* bootstrap,
    TObjectServiceConfigPtr config,
    const NRpc::TServiceDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NApi

#define OBJECT_SERVICE_INL_H_
#include "object_service-inl.h"
#undef OBJECT_SERVICE_INL_H_
