#pragma once

#include <yql/tools/yqlworker/config/config.h>
#include <yql/tools/yqlworker/executor/task_executor.h>

namespace NYql {

using TBindResultVector = TVector<NBus::TBindResult>;

int RunWorkerMain(int argc, const char* argv[]);

int RunWorkerCore(
    const TMainConfig& config,
    ITaskExecutorPtr taskExecutor,
    const TBindResultVector& serverPorts);

} // namespace NYql
