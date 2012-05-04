#include "stdafx.h"
#include "operation.h"
#include "job.h"
#include "exec_node.h"
#include "operation_controller.h"

namespace NYT {
namespace NScheduler {

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

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

