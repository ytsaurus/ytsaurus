#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/horizontal_pod_autoscaler.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class THorizontalPodAutoscaler
    : public TObject
    , public NYT::TRefTracked<THorizontalPodAutoscaler>
{
public:
    static constexpr EObjectType Type = EObjectType::HorizontalPodAutoscaler;

    THorizontalPodAutoscaler(
        const TObjectId& id,
        const TObjectId& replicaSetId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TReplicaSetAttribute = TParentAttribute<TReplicaSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TReplicaSetAttribute, ReplicaSet);

    using TSpec = NYP::NClient::NApi::NProto::THorizontalPodAutoscalerSpec;
    static const TScalarAttributeSchema<THorizontalPodAutoscaler, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::THorizontalPodAutoscalerStatus;
    static const TScalarAttributeSchema<THorizontalPodAutoscaler, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
