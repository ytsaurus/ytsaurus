#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP {
namespace NServer {
namespace NObjects {

////////////////////////////////////////////////////////////////////////////////

class TEndpoint
    : public TObject
    , public NYT::TRefTracked<TEndpoint>
{
public:
    static constexpr EObjectType Type = EObjectType::Endpoint;

    TEndpoint(
        const TObjectId& id,
        const TObjectId& endpointSetId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TEndpointSetAttribute = TParentAttribute<TEndpointSet>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TEndpointSetAttribute, EndpointSet);

    using TSpec = NYP::NClient::NApi::NProto::TEndpointSpec;
    static const TScalarAttributeSchema<TEndpoint, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NServer
} // namespace NYP
