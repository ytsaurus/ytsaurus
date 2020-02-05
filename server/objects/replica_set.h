#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/replica_set.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TReplicaSet
    : public TObject
    , public NYT::TRefTracked<TReplicaSet>
{
public:
    static constexpr EObjectType Type = EObjectType::ReplicaSet;

    TReplicaSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using THorizontalPodAutoscalerAttribute = TChildrenAttribute<THorizontalPodAutoscaler>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(THorizontalPodAutoscalerAttribute, HorizontalPodAutoscaler);

    class TSpec
    {
    public:
        explicit TSpec(TReplicaSet* replicaSet);

        static const TManyToOneAttributeSchema<TReplicaSet, TAccount> AccountSchema;
        using TAccountAttribute = TManyToOneAttribute<TReplicaSet, TAccount>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAccountAttribute, Account);

        static const TManyToOneAttributeSchema<TReplicaSet, TNodeSegment> NodeSegmentSchema;
        using TNodeSegmentAttribute = TManyToOneAttribute<TReplicaSet, TNodeSegment>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNodeSegmentAttribute, NodeSegment);

        using TEtc = NProto::TReplicaSetSpecEtc;
        static const TScalarAttributeSchema<TReplicaSet, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TReplicaSetStatus;
    static const TScalarAttributeSchema<TReplicaSet, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
