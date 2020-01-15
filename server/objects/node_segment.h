#pragma once

#include "object.h"

#include <yp/server/nodes/public.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TNodeSegment
    : public TObject
    , public TRefTracked<TNodeSegment>
{
public:
    static constexpr EObjectType Type = EObjectType::NodeSegment;

    TNodeSegment(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TSpec = NClient::NApi::NProto::TNodeSegmentSpec;
    static const TScalarAttributeSchema<TNodeSegment, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NClient::NApi::NProto::TNodeSegmentStatus;
    static const TScalarAttributeSchema<TNodeSegment, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);

    static const TOneToManyAttributeSchema<TNodeSegment, TPodSet> PodSetsSchema;
    using TPodSets = TOneToManyAttribute<TNodeSegment, TPodSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSets, PodSets);

    static const TOneToManyAttributeSchema<TNodeSegment, TReplicaSet> ReplicaSetsSchema;
    using TReplicaSets = TOneToManyAttribute<TNodeSegment, TReplicaSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReplicaSets, ReplicaSets);

    static const TOneToManyAttributeSchema<TNodeSegment, TMultiClusterReplicaSet> MultiClusterReplicaSetsSchema;
    using TMultiClusterReplicaSets = TOneToManyAttribute<TNodeSegment, TMultiClusterReplicaSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMultiClusterReplicaSets, MultiClusterReplicaSets);

    virtual bool IsBuiltin() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
