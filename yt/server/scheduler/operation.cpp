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
    const TOperationId& operationId,
    EOperationType type,
    const TTransactionId& transactionId,
    NYTree::IMapNodePtr spec,
    TInstant startTime,
    EOperationState state)
    : OperationId_(operationId)
    , Type_(type)
    , State_(state)
    , TransactionId_(transactionId)
    , Spec_(spec)
    , StartTime_(startTime)
    , FinishedPromise(NewPromise<void>())
{ }

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

