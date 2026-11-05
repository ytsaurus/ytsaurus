#pragma once

#include <contrib/ydb/library/actors/core/events.h>

#include <contrib/ydb/library/yql/providers/dq/counters/counters.h>
#include <contrib/ydb/library/yql/providers/dq/api/protos/dqs.pb.h>
#include <contrib/ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <yt/yql/providers/dq/config/config.pb.h>
#include <yql/essentials/providers/common/metrics/metrics_registry.h>

namespace NYql::NDq {

class IScheduler {
public:
    using TPtr = std::unique_ptr<IScheduler>;

    static TPtr Make(
        const NProto::TDqConfig::TScheduler& schedulerConfig = {},
        IMetricsRegistryPtr metricsRegistry = {},
        size_t targetCapacity = 0);

    virtual ~IScheduler() = default;

    struct TWaitInfo {
        const NYql::NDqProto::TAllocateWorkersRequest Request;
        const NActors::TActorId Sender;
        const TInstant StartTime;

        TCounters Stat;
        mutable THashMap<TString, int> ResLeft;

        TWaitInfo(const NYql::NDqProto::TAllocateWorkersRequest& record, const NActors::TActorId& sender);
    };

    enum class ESuspendStatus {
        Accepted,
        PerUserLimit,
        GlobalLimit,
    };

    struct TSuspendResult {
        ESuspendStatus Status = ESuspendStatus::Accepted;
        ui64 WaitingOperations = 0;
        ui64 Limit = 0;
    };

    virtual TSuspendResult Suspend(TWaitInfo&& info) = 0;

    virtual size_t GetRunningTasksPerUserLimit() const = 0;

    virtual void ReleaseRunningTasks(const TString& user, size_t count) = 0;

    virtual std::vector<NActors::TActorId> Cleanup() = 0;

    virtual size_t UpdateMetrics(bool updateRunningLimitedQueueSize = true) = 0;

    using TProcessor = std::function<bool(const TWaitInfo& info)>;

    virtual void Process(size_t totalWorkersCount, size_t freeWorkersCount, const TProcessor& processor, const TInstant& timestamp = TInstant::Now()) = 0;

    virtual void ProcessAll(const TProcessor& processor) = 0;

    virtual void ForEach(const std::function<void(const TWaitInfo& info)>& processor) = 0;
};

}
