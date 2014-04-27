#pragma once

#include "executor.h"

#include <core/misc/nullable.h>

#include <ytlib/driver/driver.h>

#include <core/ytree/yson_string.h>

#include <server/job_proxy/public.h>

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

    void Run();

private:
    TExecutorConfigPtr Config;
    NDriver::IDriverPtr Driver;
    NScheduler::TOperationId OperationId;
    NScheduler::EOperationType OperationType;
    TNullable<NYTree::TYsonString> PrevProgress;

    static void AppendPhaseProgress(Stroka* out, const Stroka& phase, const NYTree::TYsonString& progress);

    Stroka FormatProgress(const NYTree::TYsonString& progress);
    void DumpProgress();
    void DumpResult();

    NScheduler::EOperationType GetOperationType(const NScheduler::TOperationId& operationId);

    bool CheckFinished();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

