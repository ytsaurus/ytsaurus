#include "dq_scheduler.h"

#include <queue>
#include <list>
#include <tuple>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql::NDq {

IScheduler::TWaitInfo::TWaitInfo(const NYql::NDqProto::TAllocateWorkersRequest& record, const NActors::TActorId& sender)
    : Request(record), Sender(sender), StartTime(TInstant::Now())
{ }

namespace {

struct TMyCounters {
    using TPtr = std::unique_ptr<TMyCounters>;

    const TSensorsGroupPtr Group;
    const NMonitoring::TDynamicCounters::TCounterPtr QueueSizeForSmall;
    const NMonitoring::TDynamicCounters::TCounterPtr QueueSizeForLarge;
    const NMonitoring::TDynamicCounters::TCounterPtr IntegralQueueSizeForLarge;
    const NMonitoring::TDynamicCounters::TCounterPtr AllocatedTotal;
    const NMonitoring::TDynamicCounters::TCounterPtr RunningTotal;
    const NMonitoring::TDynamicCounters::TCounterPtr RunningLimitedQueueSize;
    const NMonitoring::TDynamicCounters::TCounterPtr KnownUsers;
    const NMonitoring::TDynamicCounters::TCounterPtr UserLimited;
    const NMonitoring::TDynamicCounters::TCounterPtr PerUserQueueLimitRejections;
    const NMonitoring::TDynamicCounters::TCounterPtr GlobalQueueLimitRejections;

    TMyCounters(TSensorsGroupPtr&& group)
        : Group(std::move(group))
        , QueueSizeForSmall(Group->GetCounter("QueueSizeForSmall"))
        , QueueSizeForLarge(Group->GetCounter("QueueSizeForLarge"))
        , IntegralQueueSizeForLarge(Group->GetCounter("IntegralQueueSizeForLarge"))
        , AllocatedTotal(Group->GetCounter("AllocatedTotal"))
        , RunningTotal(Group->GetCounter("RunningTotal"))
        , RunningLimitedQueueSize(Group->GetCounter("RunningLimitedQueueSize"))
        , KnownUsers(Group->GetCounter("KnownUsers"))
        , UserLimited(Group->GetCounter("UserLimited", /*derivative=*/ true))
        , PerUserQueueLimitRejections(Group->GetCounter("PerUserQueueLimitRejections", /*derivative=*/ true))
        , GlobalQueueLimitRejections(Group->GetCounter("GlobalQueueLimitRejections", /*derivative=*/ true))
    {}

    static TPtr Make(IMetricsRegistryPtr metricsRegistry) {
        return metricsRegistry ? std::make_unique<TMyCounters>(metricsRegistry->GetSensors()->GetSubgroup("component", "scheduler")) : nullptr;
    }
};

size_t CalculateRunningTasksPerUserLimit(size_t targetCapacity, ui32 percent)
{
    if (percent > 100) {
        YQL_CLOG(ERROR, ProviderDq)
            << "Running tasks per-user limit exceeds 100 percent; clamping to 100"
            << " (Percent: " << percent << ")";
        percent = 100;
    }
    if (percent == 0) {
        return 0;
    }
    if (targetCapacity == 0) {
        YQL_CLOG(ERROR, ProviderDq)
            << "Target capacity is zero; disabling running tasks per-user limit"
            << " (Percent: " << percent << ")";
        return 0;
    }

    // Divide before multiplying so targetCapacity * percent cannot overflow size_t;
    // the remainder term rounds the limit up.
    const auto quotient = targetCapacity / 100;
    const auto remainder = targetCapacity % 100;
    return quotient * percent + (remainder * percent + 99) / 100;
}

class TScheduler : public IScheduler {
public:
    TScheduler(
        IMetricsRegistryPtr metricsRegistry,
        const NProto::TDqConfig::TScheduler& config,
        size_t targetCapacity)
        : KeepReserveForLiteralRequests(config.GetKeepReserveForLiteralRequests())
        , HistoryKeepingTime(TDuration::Minutes(config.GetHistoryKeepingTime()))
        , MaxOperations(config.GetMaxOperations())
        , MaxOperationsPerUser(config.GetMaxOperationsPerUser())
        , EnablePerUserMetrics(config.GetEnablePerUserMetrics())
        , RunningTasksPerUserLimit(CalculateRunningTasksPerUserLimit(
            targetCapacity,
            config.GetLimitRunningTasksPerUserPercent()))
        , Counters(TMyCounters::Make(metricsRegistry))
        , EnableLimiter(config.GetLimitTasksPerWindow())
        , LimiterNumerator(config.GetLimiterNumerator())
        , LimiterDenumerator(config.GetLimiterDenumerator())
    { }

private:
    class TUserInfo {
    public:
        TUserInfo(
            i64& allocatedTotal,
            i64& runningTotal,
            TMyCounters* schedulerCounters,
            const TString& user)
            : AllocatedTotal(allocatedTotal)
            , RunningTotal(runningTotal)
            , SchedulerCounters(schedulerCounters)
            , User(user)
        {
            if (SchedulerCounters) {
                const auto group = SchedulerCounters->Group->GetSubgroup("user", User);
                Counters.Await = group->GetCounter("Await");
                Counters.AwaitOperations = group->GetCounter("AwaitOperations");
                Counters.Allocated = group->GetCounter("Allocated");
            }
        }

        ~TUserInfo()
        {
            Cleanup();
            if (SchedulerCounters) {
                SchedulerCounters->Group->RemoveSubgroup("user", User);
            }
        }

        ui64 GetAwaitOperations() const
        {
            return AwaitOperations;
        }

        ui64 GetAllocated() const
        {
            return Allocated;
        }

        ui64 GetRunning() const
        {
            return Running;
        }

        bool CanBeRemoved() const
        {
            return Await == 0 &&
                Allocated == 0 &&
                AwaitOperations == 0 &&
                Running == 0 &&
                History.empty();
        }

        void Enqueue(ui32 count)
        {
            Await += count;
            AwaitOperations += 1;
            UpdateAwaitCounters();
        }

        void Allocate(ui32 count, const TInstant& now)
        {
            Await -= count;
            AwaitOperations -= 1;
            Allocated += count;
            AllocatedTotal += count;
            Running += count;
            RunningTotal += count;
            History.emplace(now, count);
            UpdateAwaitCounters();
            UpdateAllocatedCounter();
        }

        void Dequeue(ui32 count)
        {
            Await -= count;
            AwaitOperations -= 1;
            UpdateAwaitCounters();
        }

        ui64 ReleaseRunningTasks(size_t count)
        {
            const auto releasedCount = std::min<ui64>(Running, count);
            Running -= releasedCount;
            RunningTotal -= releasedCount;
            return releasedCount;
        }

        void ExpireAllocations(const TInstant& from)
        {
            ui64 expired = 0;
            while (!History.empty() && History.front().first <= from) {
                expired += History.front().second;
                History.pop();
            }
            if (expired) {
                Allocated -= expired;
                AllocatedTotal -= expired;
                UpdateAllocatedCounter();
            }
        }

    private:
        // References to the owning TScheduler's totals.
        i64& AllocatedTotal;
        i64& RunningTotal;
        TMyCounters* const SchedulerCounters;
        const TString User;

        ui64 Await = 0ULL;
        ui64 Allocated = 0ULL;
        ui64 AwaitOperations = 0ULL;
        ui64 Running = 0ULL;
        std::queue<std::pair<TInstant, ui32>> History;

        struct {
            NMonitoring::TDynamicCounters::TCounterPtr Await;
            NMonitoring::TDynamicCounters::TCounterPtr AwaitOperations;
            NMonitoring::TDynamicCounters::TCounterPtr Allocated;
        } Counters;

        void UpdateAwaitCounters()
        {
            if (Counters.Await) {
                Counters.Await->Set(Await);
                Counters.AwaitOperations->Set(AwaitOperations);
            }
        }

        void UpdateAllocatedCounter()
        {
            if (Counters.Allocated) {
                Counters.Allocated->Set(Allocated);
            }
        }

        void Cleanup()
        {
            AllocatedTotal -= Allocated;
            RunningTotal -= Running;
            Await = 0;
            Allocated = 0;
            AwaitOperations = 0;
            Running = 0;
            UpdateAwaitCounters();
            UpdateAllocatedCounter();
        }
    };

    using THistoryMap = THashMap<TString, TUserInfo>;

    struct TFullWaitInfo : public TWaitInfo {
        TFullWaitInfo(TWaitInfo&& info, TUserInfo* userInfo)
            : TWaitInfo(std::move(info)), UserInfo(userInfo)
        {}

        TUserInfo* const UserInfo;
    };

    bool IsRunningLimited(const TFullWaitInfo& info) const {
        return RunningTasksPerUserLimit > 0 &&
            info.UserInfo->GetRunning() + info.Request.GetCount() > RunningTasksPerUserLimit;
    }

    bool Suspend(TWaitInfo&& info) final {
        const auto& user = info.Request.GetUser();
        // Reuse the lookup context for insertion to avoid hashing a new user twice.
        THistoryMap::insert_ctx insertCtx = nullptr;
        auto userIt = AllocationsHistory.find(user, insertCtx);
        if (info.Request.GetCount() > 1U) {
            const auto userAwaitOperations = userIt == AllocationsHistory.end()
                ? 0ULL
                : userIt->second.GetAwaitOperations();
            if (userAwaitOperations >= MaxOperationsPerUser) {
                if (Counters) {
                    *Counters->PerUserQueueLimitRejections += 1;
                }
                return false;
            }
            if (LargeWaitList.size() >= MaxOperations) {
                if (Counters) {
                    *Counters->GlobalQueueLimitRejections += 1;
                }
                return false;
            }
        }

        if (userIt == AllocationsHistory.end()) {
            userIt = AllocationsHistory.emplace_direct(
                insertCtx,
                std::piecewise_construct,
                std::forward_as_tuple(user),
                std::forward_as_tuple(
                    AllocatedTotal,
                    RunningTotal,
                    EnablePerUserMetrics ? Counters.get() : nullptr,
                    user));
        }

        userIt->second.Enqueue(info.Request.GetCount());
        (info.Request.GetCount() > 1U ? LargeWaitList : SmallWaitList).emplace_back(
            std::move(info),
            &userIt->second);
        return true;
    }

    size_t GetRunningTasksPerUserLimit() const final {
        return RunningTasksPerUserLimit;
    }

    void ReleaseRunningTasks(const TString& user, size_t count) final {
        const auto userIt = AllocationsHistory.find(user);
        if (userIt == AllocationsHistory.end()) {
            YQL_CLOG(ERROR, ProviderDq)
                << "Cannot release running tasks for unknown user"
                << " (User: " << user << ", Count: " << count << ")";
            return;
        }

        const auto userRunning = userIt->second.GetRunning();
        const auto runningTotal = RunningTotal;
        const auto releasedCount = userIt->second.ReleaseRunningTasks(count);
        if (releasedCount != count) {
            YQL_CLOG(ERROR, ProviderDq)
                << "Released fewer running tasks than requested"
                << " (User: " << user
                << ", Count: " << count
                << ", ReleasedCount: " << releasedCount
                << ", UserRunning: " << userRunning
                << ", RunningTotal: " << runningTotal << ")";
        }
    }

    std::vector<NActors::TActorId> Cleanup() final {
        std::vector<NActors::TActorId> senders;
        senders.reserve(SmallWaitList.size() + LargeWaitList.size());
        std::transform(SmallWaitList.cbegin(), SmallWaitList.cend(), std::back_inserter(senders), [](const TWaitInfo& info) { return info.Sender; });
        std::transform(LargeWaitList.cbegin(), LargeWaitList.cend(), std::back_inserter(senders), [](const TWaitInfo& info) { return info.Sender; });
        SmallWaitList.clear();
        LargeWaitList.clear();
        AllocationsHistory.clear();
        if (Counters) {
            Counters->RunningLimitedQueueSize->Set(0);
        }
        return senders;
    }

    size_t UpdateMetrics(bool updateRunningLimitedQueueSize) final {
        if (Counters) {
            Counters->KnownUsers->Set(AllocationsHistory.size());
            Counters->AllocatedTotal->Set(AllocatedTotal);
            Counters->RunningTotal->Set(RunningTotal);

            const bool collectRunningLimitedQueueSize =
                RunningTasksPerUserLimit > 0 && updateRunningLimitedQueueSize;
            size_t runningLimitedSmallQueueSize = 0;
            size_t runningLimitedLargeQueueSize = 0;
            ui64 integralQueueSizeForLarge = 0;

            if (collectRunningLimitedQueueSize) {
                runningLimitedSmallQueueSize = std::count_if(
                    SmallWaitList.cbegin(),
                    SmallWaitList.cend(),
                    [this] (const auto& info) {
                        return IsRunningLimited(info);
                    });
            }

            for (const auto& info : LargeWaitList) {
                integralQueueSizeForLarge += info.Request.GetCount();
                if (collectRunningLimitedQueueSize && IsRunningLimited(info)) {
                    ++runningLimitedLargeQueueSize;
                }
            }

            if (RunningTasksPerUserLimit == 0) {
                Counters->RunningLimitedQueueSize->Set(0);
            } else if (updateRunningLimitedQueueSize) {
                Counters->RunningLimitedQueueSize->Set(
                    runningLimitedSmallQueueSize + runningLimitedLargeQueueSize);
            }
            Counters->QueueSizeForSmall->Set(SmallWaitList.size());
            Counters->QueueSizeForLarge->Set(LargeWaitList.size());
            Counters->IntegralQueueSizeForLarge->Set(integralQueueSizeForLarge);
        }

        return SmallWaitList.size() + LargeWaitList.size();
    }

    void Process(size_t total, size_t count, const TProcessor& processor, const TInstant& now) final {
        const auto from = now - HistoryKeepingTime;
        EraseNodesIf(AllocationsHistory, [from] (auto& item) {
            auto& userInfo = item.second;
            userInfo.ExpireAllocations(from);
            return userInfo.CanBeRemoved();
        });

        const auto sort = [](const TFullWaitInfo& lhs, const TFullWaitInfo& rhs) {
            return lhs.UserInfo->GetAllocated() < rhs.UserInfo->GetAllocated();
        };

        SmallWaitList.sort(sort);
        LargeWaitList.sort(sort);

        const auto processWaitList = [&processor, &now, &total, this] (
            size_t& quota,
            std::list<TFullWaitInfo>& list)
        {
            bool needsReserve = false;
            list.remove_if([&](const TFullWaitInfo& info) {
                const auto count = info.Request.GetCount();
                if (IsRunningLimited(info)) {
                    return false;
                }

                if (quota < count) {
                    needsReserve = true;
                    return false;
                }

                if (EnableLimiter && (info.UserInfo->GetAllocated() + count) > LimiterNumerator * total / LimiterDenumerator) {
                    if (Counters) {
                        *Counters->UserLimited += 1;
                    }
                    needsReserve = true;
                    return false;
                }

                if (processor(info)) {
                    info.UserInfo->Allocate(count, now);
                    quota -= count;
                    return true;
                }

                needsReserve = true;
                return false;
            });
            return needsReserve;
        };

        const auto full = count;
        const auto half = full >> 1U;

        bool smallNeedsReserve = false;
        if (count -= half) {
            smallNeedsReserve = processWaitList(count, SmallWaitList);
        }

        if (KeepReserveForLiteralRequests) {
            count = !smallNeedsReserve && count + half >= total >> 2U
                ? std::min(count + half, full - (half >> 1U))
                : half;
        } else {
            count += half;
        }

        bool largeNeedsReserve = false;
        if (count) {
            largeNeedsReserve = processWaitList(count, LargeWaitList);
        }

        if (count && !largeNeedsReserve) {
            processWaitList(count, SmallWaitList);
        }
    }

    void ProcessAll(const TProcessor& processor) final {
        const auto proc = [&processor](const TFullWaitInfo& info) {
            if (processor(info)) {
                info.UserInfo->Dequeue(info.Request.GetCount());
                return true;
            }
            return false;
        };

        SmallWaitList.remove_if(proc);
        LargeWaitList.remove_if(proc);
    }

    void ForEach(const std::function<void(const TWaitInfo& info)>& processor) final {
        std::for_each(SmallWaitList.cbegin(), SmallWaitList.cend(), processor);
        std::for_each(LargeWaitList.cbegin(), LargeWaitList.cend(), processor);
    }

    const bool KeepReserveForLiteralRequests;
    const TDuration HistoryKeepingTime;
    const size_t MaxOperations;
    const size_t MaxOperationsPerUser;
    const bool EnablePerUserMetrics;
    const size_t RunningTasksPerUserLimit;

    i64 AllocatedTotal = 0;
    i64 RunningTotal = 0;

    const TMyCounters::TPtr Counters;

    THistoryMap AllocationsHistory;
    std::list<TFullWaitInfo> SmallWaitList, LargeWaitList;

    bool EnableLimiter;
    ui32 LimiterNumerator;
    ui32 LimiterDenumerator;
};

}

IScheduler::TPtr IScheduler::Make(
    const NProto::TDqConfig::TScheduler& config,
    IMetricsRegistryPtr metricsRegistry,
    size_t targetCapacity)
{
    return std::make_unique<TScheduler>(metricsRegistry, config, targetCapacity);
}

}
