#include "account.h"

namespace NYP {
namespace NServer {
namespace NScheduler {

using namespace NObjects;

using namespace NYT::NYson;

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(
    const TObjectId& id,
    TYsonString labels)
    : TObject(id, std::move(labels))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NObjects
} // namespace NScheduler
} // namespace NYP

