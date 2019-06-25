#include "account.h"

namespace NYP::NServer::NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(
    const TObjectId& id,
    TYsonString labels)
    : TObject(id, std::move(labels))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NScheduler
