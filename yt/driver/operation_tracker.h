#pragma once

#include "executor.h"

#include <ytlib/job_proxy/config.h>
#include <ytlib/driver/driver.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TOperationTracker
{
public:
    TOperationTracker(
        TExecutorConfigPtr config,
        NDriver::IDriverPtr driver,
        const NScheduler::TOperationId& operationId);

    EExitCode Run();

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

    static void AppendPhaseProgress(Stroka* out, const Stroka& phase, const NYTree::TYsonString& progress);

    Stroka FormatProgress(const NYTree::TYsonString& progress);
    void DumpProgress();
    EExitCode DumpResult();

    NScheduler::EOperationType GetOperationType(const NScheduler::TOperationId& operationId);

    bool IsOperationFinished();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

