#include "account.h"

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(
    TObjectId id,
    NYT::NYson::TYsonString labels,
    TObjectId parentId)
    : TObject(std::move(id), std::move(labels))
    , ParentId_(std::move(parentId))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
