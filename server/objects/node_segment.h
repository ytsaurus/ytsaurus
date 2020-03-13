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

    using TPodSetsAttribute = TOneToManyAttribute<TNodeSegment, TPodSet>;
    static const TPodSetsAttribute::TSchema PodSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSetsAttribute, PodSets);

    using TReplicaSetsAttribute = TOneToManyAttribute<TNodeSegment, TReplicaSet>;
    static const TReplicaSetsAttribute::TSchema ReplicaSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReplicaSetsAttribute, ReplicaSets);

    using TMultiClusterReplicaSetsAttribute = TOneToManyAttribute<TNodeSegment, TMultiClusterReplicaSet>;
    static const TMultiClusterReplicaSetsAttribute::TSchema MultiClusterReplicaSetsSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TMultiClusterReplicaSetsAttribute, MultiClusterReplicaSets);

    virtual bool IsBuiltin() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
