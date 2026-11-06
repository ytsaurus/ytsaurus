#pragma once

#include <yql/tools/yqlworker/executor/task_executor.h>

#include <library/cpp/messagebus/network.h>

namespace NYql {

struct TMainConfig;

using TBindResultVector = TVector<NBus::TBindResult>;

class TYqlWorkerCore {
public:
    TYqlWorkerCore(
            const TMainConfig& config,
            ITaskExecutorPtr taskExecutor,
            const TBindResultVector& serverPorts);
    ~TYqlWorkerCore();

    int Run();

    static TBindResultVector BindOn(int port);

private:
    struct TImpl;
    THolder<TImpl> Impl_;
};

} // namespace NYql
