#pragma once

#include "object.h"

#include <yp/server/objects/proto/autogen.pb.h>

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TEndpointSet
    : public TObject
    , public NYT::TRefTracked<TEndpointSet>
{
public:
    static constexpr EObjectType Type = EObjectType::EndpointSet;

    TEndpointSet(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TEndpointsAttribute = TChildrenAttribute<TEndpoint>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TEndpointsAttribute, Endpoints);

    using TSpec = NYP::NClient::NApi::NProto::TEndpointSetSpec;
    static const TScalarAttributeSchema<TEndpointSet, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    class TStatus
    {
    public:
        explicit TStatus(TEndpointSet* endpointSet);

        static const TTimestampAttributeSchema LastEndpointsUpdateTimestampSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TTimestampAttribute, LastEndpointsUpdateTimestamp);

        using TEtc = NProto::TEndpointSetStatusEtc;
        static const TScalarAttributeSchema<TEndpointSet, TEtc> EtcSchema;
        DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TEtc>, Etc);
    };
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
