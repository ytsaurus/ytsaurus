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
    TInstant startTime)
    : OperationId_(operationId)
    , Type_(type)
    , TransactionId_(transactionId)
    , Spec_(spec)
    , StartTime_(startTime)
    , FinishPromise()
{ }

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

