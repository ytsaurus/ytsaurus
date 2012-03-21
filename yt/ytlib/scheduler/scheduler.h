#pragma once

#include "public.h"

#include <ytlib/cell_scheduler/public.h>
#include <ytlib/ytree/public.h>
#include <ytlib/misc/error.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    void Start();

    TFuture< TValueOrError<TOperationPtr> >::TPtr StartOperation(
        EOperationType type,
        const TTransactionId& transactionId,
        const NYTree::IMapNodePtr spec);

    void AbortOperation(TOperationPtr operation);

    //void Schedule(
    //    TExecNodePtr node,
    //    )

private:
    class TImpl;
    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

