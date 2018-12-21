#include "internet_address.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TInternetAddress::TInternetAddress(
    const TObjectId& id,
    NYT::NYson::TYsonString labels,
    NClient::NApi::NProto::TInternetAddressSpec spec,
    NClient::NApi::NProto::TInternetAddressStatus status)
    : TObject(id, std::move(labels))
    , Spec_(spec)
    , Status_(status)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
