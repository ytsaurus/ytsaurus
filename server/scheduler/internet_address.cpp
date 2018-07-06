#include "internet_address.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

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

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
