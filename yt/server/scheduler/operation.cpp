#include "stdafx.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"

#include <ytlib/scheduler/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const NMetaState::TMutationId& mutationId,
    ITransactionPtr userTransaction,
    NYTree::IMapNodePtr spec,
    const Stroka& authenticatedUser,
    TInstant startTime,
    EOperationState state,
    bool suspended)
    : Id_(id)
    , Type_(type)
    , MutationId_(mutationId)
    , State_(state)
    , Suspended_(suspended)
    , UserTransaction_(userTransaction)
    , Spec_(spec)
    , AuthenticatedUser_(authenticatedUser)
    , StartTime_(startTime)
    , StdErrCount_(0)
    , MaxStdErrCount_(0)
    , CleanStart_(false)
    , StartedPromise(NewPromise<TError>())
    , FinishedPromise(NewPromise())
{ }

TFuture<TOperationStartResult> TOperation::GetStarted()
{
    auto this_ = MakeStrong(this);
    return StartedPromise.ToFuture().Apply(BIND([this_] (TError error) -> TOperationStartResult {
        if (error.IsOK()) {
            return TOperationStartResult(this_);
        } else {
            return error;
        }
    }));
}

void TOperation::SetStarted(const TError& error)
{
    StartedPromise.Set(error);
}

TFuture<void> TOperation::GetFinished()
{
    return FinishedPromise;
}

void TOperation::SetFinished()
{
    FinishedPromise.Set();
}

bool TOperation::IsFinishedState() const
{
    return IsOperationFinished(State_);
}

bool TOperation::IsFinishingState() const
{
    return IsOperationFinishing(State_);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

