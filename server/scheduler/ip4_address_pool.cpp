#include "ip4_address_pool.h"

namespace NYP::NServer::NScheduler {

////////////////////////////////////////////////////////////////////////////////

TIP4AddressPool::TIP4AddressPool(
    const TObjectId& id,
    NYT::NYson::TYsonString labels,
    NClient::NApi::NProto::TIP4AddressPoolSpec spec,
    NClient::NApi::NProto::TIP4AddressPoolStatus status)
    : TObject(id, std::move(labels))
    , Spec_(spec)
    , Status_(status)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
