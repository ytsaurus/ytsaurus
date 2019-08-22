#include "internet_address.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TInternetAddress::TInternetAddress(
    TObjectId id,
    TObjectId ip4AddressPoolId,
    NYT::NYson::TYsonString labels,
    NClient::NApi::NProto::TInternetAddressSpec spec,
    NClient::NApi::NProto::TInternetAddressStatus status)
    : TObject(std::move(id), std::move(labels))
    , ParentId_(std::move(ip4AddressPoolId))
    , Spec_(spec)
    , Status_(status)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
