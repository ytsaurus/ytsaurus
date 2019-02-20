#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>
#include <yp/client/api/proto/dynamic_resource.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TDynamicResource
    : public TObject
    , public NYT::TRefTracked<TDynamicResource>
{
public:
    static constexpr EObjectType Type = EObjectType::DynamicResource;

    TDynamicResource(
        const TObjectId& id,
        const TObjectId& podSetId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TPodSetAttribute = TParentAttribute<TPodSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TPodSetAttribute, PodSet);

    using TSpec = NYP::NClient::NApi::NProto::TDynamicResourceSpec;
    static const TScalarAttributeSchema<TDynamicResource, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TDynamicResourceStatus;
    static const TScalarAttributeSchema<TDynamicResource, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
