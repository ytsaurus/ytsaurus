#include "operation.h"
#include "exec_node.h"
#include "job.h"
#include "operation_controller.h"

#include <yt/ytlib/scheduler/helpers.h>

namespace NYT {
namespace NScheduler {

using namespace NApi;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const TMutationId& mutationId,
    ITransactionPtr userTransaction,
    IMapNodePtr spec,
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
    , Activated_(false)
    , Prepared_(false)
    , UserTransaction_(userTransaction)
    , HasActiveTransactions_(false)
    , Spec_(spec)
    , AuthenticatedUser_(authenticatedUser)
    , Owners_(owners)
    , StartTime_(startTime)
    , StderrCount_(0)
    , MaxStderrCount_(0)
    , CodicilData_(Format("OperationId: %v", Id_))
{ }

TFuture<TOperationPtr> TOperation::GetStarted()
{
    return StartedPromise_.ToFuture().Apply(BIND([this_ = MakeStrong(this)] () -> TOperationPtr {
        return this_;
    }));
}

void TOperation::SetStarted(const TError& error)
{
    StartedPromise_.Set(error);
}

TFuture<void> TOperation::GetFinished()
{
    return FinishedPromise_;
}

void TOperation::SetFinished()
{
    FinishedPromise_.Set();
}

bool TOperation::IsFinishedState() const
{
    return IsOperationFinished(State_);
}

bool TOperation::IsFinishingState() const
{
    return IsOperationFinishing(State_);
}

bool TOperation::IsSchedulable() const
{
    return State_ == EOperationState::Running && !Suspended_;
}

void TOperation::UpdateControllerTimeStatistics(const NYPath::TYPath& name, TDuration value)
{
    ControllerTimeStatistics_.AddSample(name, value.MicroSeconds());
}

bool TOperation::HasControllerProgress() const
{
    return (State_ == EOperationState::Running || IsFinishedState()) &&
        Controller_ &&
        Controller_->HasProgress();
}

TCodicilGuard TOperation::MakeCodicilGuard()
{
    return TCodicilGuard(CodicilData_);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

