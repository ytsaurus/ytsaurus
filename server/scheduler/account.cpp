#include "account.h"

namespace NYP::NServer::NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(
    TObjectId id,
    TYsonString labels,
    TObjectId parentId)
    : TObject(std::move(id), std::move(labels))
    , ParentId_(std::move(parentId))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
