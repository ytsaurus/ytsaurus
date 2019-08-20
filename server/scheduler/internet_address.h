#pragma once

#include "object.h"

#include <yp/client/api/proto/data_model.pb.h>

#include <yt/core/misc/ref_tracked.h>

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TInternetAddress
    : public TObject
    , public NYT::TRefTracked<TInternetAddress>
{
public:
    TInternetAddress(
        const TObjectId& id,
        const TObjectId& ip4AddressPoolId,
        NYT::NYson::TYsonString labels,
        NClient::NApi::NProto::TInternetAddressSpec spec,
        NClient::NApi::NProto::TInternetAddressStatus status);

    DEFINE_BYVAL_RO_PROPERTY(TObjectId, ParentId);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TInternetAddressSpec, Spec);
    DEFINE_BYREF_RO_PROPERTY(NClient::NApi::NProto::TInternetAddressStatus, Status);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
