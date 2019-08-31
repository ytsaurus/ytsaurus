#include "ip4_address_pool.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TIP4AddressPool::TIP4AddressPool(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    NClient::NApi::NProto::TIP4AddressPoolSpec spec,
    NClient::NApi::NProto::TIP4AddressPoolStatus status)
    : TObject(std::move(id), std::move(labels))
    , Spec_(spec)
    , Status_(status)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
