#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TIP4AddressPool
    : public TObject
    , public NYT::TRefTracked<TIP4AddressPool>
{
public:
    static constexpr EObjectType Type = EObjectType::IP4AddressPool;

    TIP4AddressPool(
        const TObjectId& id,
        IObjectTypeHandler* typeHandler,
        ISession* session);

    virtual EObjectType GetType() const override;

    using TInternetAddressesAttribute = TChildrenAttribute<TInternetAddress>;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TInternetAddressesAttribute, InternetAddresses);

    using TSpec = NYP::NClient::NApi::NProto::TIP4AddressPoolSpec;
    static const TScalarAttributeSchema<TIP4AddressPool, TSpec> SpecSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TSpec>, Spec);

    using TStatus = NYP::NClient::NApi::NProto::TIP4AddressPoolStatus;
    static const TScalarAttributeSchema<TIP4AddressPool, TStatus> StatusSchema;
    DEFINE_BYREF_RW_PROPERTY_NO_INIT(TScalarAttribute<TStatus>, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
