#pragma once

#include "object.h"

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

    using TSpec = NYP::NClient::NApi::NProto::TReplicaSetSpec;
    static const TScalarAttributeSchema<TReplicaSet, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TReplicaSetStatus;
    static const TScalarAttributeSchema<TReplicaSet, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
