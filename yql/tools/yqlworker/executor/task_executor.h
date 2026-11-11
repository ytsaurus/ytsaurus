#pragma once

#include <yql/tools/yqlworker/executor/common/runnable_tasks.h>
#include <yql/tools/yqlworker/misc/duplex_ipc_channel.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>
#include <yql/tools/yqlworker/proto/worker.pb.h>
#include <library/cpp/messagebus/network.h>
#include <util/generic/ptr.h>


namespace NYql {
namespace NProto {
    class TTaskExecutorConfig;
} // namespace NProto

class TGatewaysConfig;
class TFileStorageConfig;

// TODO (jamel): do not use response enums
// TODO (jamel): rename Run -> Process|Execute
using EProcessTaskStatus = NProto::TProcessTaskResponse_EStatus;
using EKillTaskStatus = NProto::TKillTaskResponse_EStatus;

inline bool IsTerminalTaskStatus(NProto::ETaskStatus status) {
    return status == NProto::COMPLETED ||
            status == NProto::ERROR ||
            status == NProto::ABORTED;
}

inline bool IsHealthCheckTask(TStringBuf taskId) {
    return taskId.StartsWith(TStringBuf("__HEALTH_CHECK__"));
}

//////////////////////////////////////////////////////////////////////////////
// ITaskExecutor
//////////////////////////////////////////////////////////////////////////////
class ITaskExecutor: public TThrRefBase {
public:
    virtual ~ITaskExecutor() = default;

    using TOrphanTaskWatcherFactory = std::function<ITaskWatcherPtr(const TString& taskId, const TString& username)>;

    virtual void Startup(TOrphanTaskWatcherFactory factory = {}) = 0;
    virtual void Shutdown(std::function<void()> onIdle = {}) = 0;

    virtual EProcessTaskStatus ProcessTask(
            NProto::ETaskAction action,
            TTaskDataPtr data,
            ITaskWatcherPtr watcher) = 0;

    virtual EKillTaskStatus KillTask(const TString& taskId) = 0;

    virtual bool IsReady() = 0;
    virtual bool HasRunningTask(const TString& taskId) const = 0;
    virtual ITaskWatcherPtr GetRunningTaskWatcher(const TString& taskId) const = 0;
    virtual ui32 GetRunningTasksCount() const = 0;
};

using ITaskExecutorPtr = TIntrusivePtr<ITaskExecutor>;

ITaskExecutorPtr CreateInprocessTaskExecutor(
        const NProto::TTaskExecutorConfig& executorConfig,
        const TMaybe<NProto::TDqConfig>& dqConfig,
        const TGatewaysConfig& gatewaysConfig,
        const TStaticGatewaysConfig& staticGatewaysConfig,
        const TFileStorageConfig& fsConfig,
        IMetricsRegistryPtr metrics);

ITaskExecutorPtr CreateSubprocessTaskExecutor(
        const NProto::TTaskExecutorConfig& executorConfig,
        TIpcChannelEndpoint& forkerProcessCh,
        IMetricsRegistryPtr metrics,
        const TVector<NBus::TBindResult>& serverPorts);

} // namespace NYql
