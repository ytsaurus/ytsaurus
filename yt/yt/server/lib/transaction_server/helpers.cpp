#include "helpers.h"

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/cypress_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTransactionServer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NProfiling;
using namespace NYTree;

///////////////////////////////////////////////////////////////////////////////

TError CreateNoSuchTransactionError(TTransactionId transactionId)
{
    return TError(
        NTransactionClient::EErrorCode::NoSuchTransaction,
        "No such transaction %v",
        transactionId);
}

void ThrowNoSuchTransaction(TTransactionId transactionId)
{
    THROW_ERROR(CreateNoSuchTransactionError(transactionId));
}

TError CreatePrerequisiteCheckFailedNoSuchTransactionError(TTransactionId transactionId)
{
    return TError(
        NObjectClient::EErrorCode::PrerequisiteCheckFailed,
        "Prerequisite check failed: transaction %v is missing",
        transactionId);
}

[[noreturn]] void ThrowPrerequisiteCheckFailedNoSuchTransaction(TTransactionId transactionId)
{
    THROW_ERROR(CreatePrerequisiteCheckFailedNoSuchTransactionError(transactionId));
}

////////////////////////////////////////////////////////////////////////////////

void LockNodeWithWait(
    const IClientPtr& client,
    const ITransactionPtr& transaction,
    const TString& lockPath,
    TDuration checkBackoff,
    TDuration waitTimeout)
{
    auto lockOrError = WaitFor(transaction->LockNode(lockPath, ELockMode::Exclusive, TLockNodeOptions{.Waitable = true}));
    if (!lockOrError.IsOK()) {
        if (!lockOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION("Failed to acquire lock")
                << TErrorAttribute("lock_path", lockPath)
                << lockOrError;
        }

        // Try to create lock node.
        auto resultOrError = WaitFor(client->CreateNode(lockPath, EObjectType::MapNode, TCreateNodeOptions{.Force = true}));
        if (!resultOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to create lock node")
                << TErrorAttribute("lock_path", lockPath)
                << resultOrError;
        }
        lockOrError = WaitFor(transaction->LockNode(lockPath, ELockMode::Exclusive, TLockNodeOptions{.Waitable = true}));
        if (!lockOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to acquire lock after creating it")
                << TErrorAttribute("lock_path", lockPath)
                << lockOrError;
        }
    }
    auto lockId = lockOrError.Value().LockId;

    auto deadline = GetInstant() + waitTimeout;
    while (true) {
        auto stateOrError = WaitFor(transaction->GetNode(Format("#%v/@state", lockId)));
        if (!stateOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Failed to get state of lock")
                << TErrorAttribute("lock_path", lockPath)
                << stateOrError;
        }
        if (ConvertToNode(stateOrError.Value())->AsString()->GetValue() == "acquired") {
            break;
        }
        TDelayedExecutor::WaitForDuration(checkBackoff);
        if (GetInstant() > deadline) {
            THROW_ERROR_EXCEPTION("Timeout exceeded while waiting for lock")
                << TErrorAttribute("lock_path", lockPath)
                << TErrorAttribute("timeout", waitTimeout);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
