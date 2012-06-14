#pragma once

#include "executor.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

#include <ytlib/ytree/ypath_proxy.h>

#include <ytlib/scheduler/scheduler_proxy.h>
#include <ytlib/scheduler/helpers.h>

#include <ytlib/object_server/object_service_proxy.h>

#include <util/stream/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TOperationTracker
{
public:
    TOperationTracker(
        TExecutorConfigPtr config,
        NDriver::IDriverPtr driver,
        const NScheduler::TOperationId& operationId);

    void Run();

private:
    TExecutorConfigPtr Config;
    NDriver::IDriverPtr Driver;
    NScheduler::TOperationId OperationId;
    NScheduler::EOperationType OperationType;

    // TODO(babenko): refactor
    // TODO(babenko): YPath and RPC responses currently share no base class.
    template <class TResponse>
    static void CheckResponse(TResponse response, const Stroka& failureMessage)
    {
        if (response->IsOK())
            return;

        ythrow yexception() << failureMessage + "\n" + response->GetError().ToString();
    }

    static void AppendPhaseProgress(Stroka* out, const Stroka& phase, const NYTree::TYson& progress);

    Stroka FormatProgress(const NYTree::TYson& progress);
    void DumpProgress();
    void DumpResult();

    NScheduler::EOperationType GetOperationType(const NScheduler::TOperationId& operationId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

