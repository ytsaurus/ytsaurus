#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/guid.h>
#include <ytlib/misc/enum.h>
#include <ytlib/transaction_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

using NTransactionServer::TTransactionId;

typedef TGuid TJobId;
typedef TGuid TOperationId;

DECLARE_ENUM(EOperationType,
    ((Map)(0))
);

DECLARE_ENUM(EJobType,
    ((Map)(0))
);

DECLARE_ENUM(EOperationState,
    ((Prepaing)(0))
);

DECLARE_ENUM(EJobState,
    ((Created)(0))
    ((PreparingProxy)(1))
    ((PreparingSandbox)(2))
    ((StartedProxy)(3))
    ((Running)(4))
    ((Completed)(5))
    ((Failed)(6))
);

class TSchedulerService;
typedef TIntrusivePtr<TSchedulerService> TSchedulerServicePtr;

class TSchedulerServiceProxy;

class TOperation;
typedef TIntrusivePtr<TOperation> TOperationPtr;

class TScheduler;
typedef TIntrusivePtr<TScheduler> TSchedulerPtr;

class TMapOperationSpec;
typedef TIntrusivePtr<TMapOperationSpec> TMapOperationSpecPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
