#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/multi_cluster_replica_set.pb.h>

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/property.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TMultiClusterReplicaSet
    : public TObject
    , public NYT::TRefTracked<TMultiClusterReplicaSet>
{
public:
    static constexpr EObjectType Type = EObjectType::MultiClusterReplicaSet;

    TMultiClusterReplicaSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    class TSpec
    {
    public:
        explicit TSpec(TMultiClusterReplicaSet* replicaSet);

        static const TManyToOneAttributeSchema<TMultiClusterReplicaSet, TAccount> AccountSchema;
        using TAccountAttribute = TManyToOneAttribute<TMultiClusterReplicaSet, TAccount>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TAccountAttribute, Account);

        static const TManyToOneAttributeSchema<TMultiClusterReplicaSet, TNodeSegment> NodeSegmentSchema;
        using TNodeSegmentAttribute = TManyToOneAttribute<TMultiClusterReplicaSet, TNodeSegment>;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TNodeSegmentAttribute, NodeSegment);

        using TEtc = NProto::TMultiClusterReplicaSetSpecEtc;
        static const TScalarAttributeSchema<TMultiClusterReplicaSet, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TSpec, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TMultiClusterReplicaSetStatus;
    static const TScalarAttributeSchema<TMultiClusterReplicaSet, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
