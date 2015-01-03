#include "stdafx.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"

#include <ytlib/scheduler/helpers.h>

#include <core/ytree/fluent.h>

namespace NYT {
namespace NScheduler {

using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////

TOperation::TOperation(
    const TOperationId& id,
    EOperationType type,
    const NHydra::TMutationId& mutationId,
    TTransactionPtr userTransaction,
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
    , StderrCount_(0)
    , MaxStderrCount_(0)
    , CleanStart_(false)
    , StartedPromise(NewPromise<TError>())
    , FinishedPromise(NewPromise())
{ }

TFuture<TOperationStartResult> TOperation::GetStarted()
{
    auto this_ = MakeStrong(this);
    return StartedPromise.ToFuture().Apply(BIND([this_] (const TError& error) -> TOperationStartResult {
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

void TOperation::UpdateStatistics(const TStatistics& statistics, EJobFinalState state)
{
    Statistics[state].Merge(statistics);
}

void TOperation::BuildStatistics(NYson::IYsonConsumer* consumer) const
{
    NYTree::BuildYsonFluently(consumer)
        .DoMapFor(TEnumTraits<EJobFinalState>::GetDomainValues(), [&] (NYTree::TFluentMap fluent, EJobFinalState state) {
            fluent
                .Item(Format("%lv_jobs", state))
                .Value(Statistics[state]);
        });
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

