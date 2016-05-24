#include "operation.h"
#include "exec_node.h"
#include "job.h"
#include "operation_controller.h"

#include <yt/ytlib/scheduler/helpers.h>

#include <yt/core/misc/common.h>

namespace NYT {
namespace NScheduler {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const NRpc::TMutationId& mutationId,
    TTransactionPtr userTransaction,
    NYTree::IMapNodePtr spec,
    const Stroka& authenticatedUser,
    const std::vector<Stroka>& owners,
    TInstant startTime,
    EOperationState state,
    bool suspended)
    : Id_(id)
    , Type_(type)
    , MutationId_(mutationId)
    , State_(state)
    , Suspended_(suspended)
    , Queued_(false)
    , UserTransaction_(userTransaction)
    , HasActiveTransactions_(false)
    , Spec_(spec)
    , AuthenticatedUser_(authenticatedUser)
    , Owners_(owners)
    , StartTime_(startTime)
    , StderrCount_(0)
    , MaxStderrCount_(0)
    , CleanStart_(false)
    , StartedPromise(NewPromise<void>())
    , FinishedPromise(NewPromise<void>())
{ }

TFuture<TOperationPtr> TOperation::GetStarted()
{
    return StartedPromise.ToFuture().Apply(BIND([this_ = MakeStrong(this)] () -> TOperationPtr {
        return this_;
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

bool TOperation::HasControllerProgress() const
{
    return (State_ == EOperationState::Running || IsFinishedState()) &&
        Controller_ &&
        Controller_->HasProgress();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

