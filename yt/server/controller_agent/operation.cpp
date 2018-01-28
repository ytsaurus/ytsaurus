#include "operation.h"

#include <yt/server/scheduler/operation.h>

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(const NScheduler::TOperation* underlying)
    : Id_(underlying->GetId())
    , Type_(underlying->GetType())
    , Spec_(underlying->GetSpec())
    , StartTime_(underlying->GetStartTime())
    , AuthenticatedUser_(underlying->GetAuthenticatedUser())
    , StorageMode_(underlying->GetStorageMode())
    , SecureVault_(underlying->GetSecureVault())
    , Owners_(underlying->GetOwners())
    , UserTransactionId_(underlying->GetUserTransactionId())
    , PoolTreeSchedulingTagFilters_(underlying->GetPoolTreeSchedulingTagFilters())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
