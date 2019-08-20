#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TInternetAddress
    : public TObject
    , public NYT::TRefTracked<TInternetAddress>
{
public:
    static constexpr EObjectType Type = EObjectType::InternetAddress;

    TInternetAddress(
        const TObjectId& id,
        const TObjectId& ip4AddressPoolId,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TIP4AddressPoolAttribute = TParentAttribute<TIP4AddressPool>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TIP4AddressPoolAttribute, IP4AddressPool);

    using TSpec = NYP::NClient::NApi::NProto::TInternetAddressSpec;
    static const TScalarAttributeSchema<TInternetAddress, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TInternetAddressStatus;
    static const TScalarAttributeSchema<TInternetAddress, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
