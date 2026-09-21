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
    const NMonitoring::TDynamicCounters::TCounterPtr KnownUsers;
    const NMonitoring::TDynamicCounters::TCounterPtr UserLimited;

    TMyCounters(TSensorsGroupPtr&& group)
        : Group(std::move(group))
        , QueueSizeForSmall(Group->GetCounter("QueueSizeForSmall"))
        , QueueSizeForLarge(Group->GetCounter("QueueSizeForLarge"))
        , IntegralQueueSizeForLarge(Group->GetCounter("IntegralQueueSizeForLarge"))
        , AllocatedTotal(Group->GetCounter("AllocatedTotal"))
        , KnownUsers(Group->GetCounter("KnownUsers"))
        , UserLimited(Group->GetCounter("UserLimited"))
    {}

    static TPtr Make(IMetricsRegistryPtr metricsRegistry) {
        return metricsRegistry ? std::make_unique<TMyCounters>(metricsRegistry->GetSensors()->GetSubgroup("component", "scheduler")) : nullptr;
    }
};

class TScheduler : public IScheduler {
public:
    TScheduler(IMetricsRegistryPtr metricsRegistry, const NProto::TDqConfig::TScheduler& config)
        : KeepReserveForLiteralRequests(config.GetKeepReserveForLiteralRequests())
        , HistoryKeepingTime(TDuration::Minutes(config.GetHistoryKeepingTime()))
        , MaxOperations(config.GetMaxOperations())
        , MaxOperationsPerUser(config.GetMaxOperationsPerUser())
        , EnablePerUserMetrics(config.GetEnablePerUserMetrics())
        , Counters(TMyCounters::Make(metricsRegistry))
        , EnableLimiter(config.GetLimitTasksPerWindow())
        , LimiterNumerator(config.GetLimiterNumerator())
        , LimiterDenumerator(config.GetLimiterDenumerator())
    {}

private:
    class TUserInfo {
    public:
        TUserInfo(i64& allocatedTotal, TMyCounters* schedulerCounters, const TString& user)
            : AllocatedTotal(allocatedTotal)
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

        ~TUserInfo() {
            Cleanup();
            if (SchedulerCounters) {
                SchedulerCounters->Group->RemoveSubgroup("user", User);
            }
        }

        ui64 GetAwaitOperations() const {
            return AwaitOperations;
        }

        ui64 GetAllocated() const {
            return Allocated;
        }

        bool CanBeRemoved() const {
            return Await == 0 && Allocated == 0 && AwaitOperations == 0 && History.empty();
        }

        void Enqueue(ui32 count) {
            Await += count;
            AwaitOperations += 1;
            UpdateAwaitCounters();
        }

        void Allocate(ui32 count, const TInstant& now) {
            Await -= count;
            AwaitOperations -= 1;
            Allocated += count;
            AllocatedTotal += count;
            History.emplace(now, count);
            UpdateAwaitCounters();
            UpdateAllocatedCounter();
        }

        void Dequeue(ui32 count) {
            Await -= count;
            AwaitOperations -= 1;
            UpdateAwaitCounters();
        }

        void ExpireAllocations(const TInstant& from) {
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
        // Reference to TScheduler::AllocatedTotal.
        i64& AllocatedTotal;
        TMyCounters* const SchedulerCounters;
        const TString User;

        ui64 Await = 0ULL;
        ui64 Allocated = 0ULL;
        ui64 AwaitOperations = 0LL;
        std::queue<std::pair<TInstant, ui32>> History;

        struct {
            NMonitoring::TDynamicCounters::TCounterPtr Await;
            NMonitoring::TDynamicCounters::TCounterPtr AwaitOperations;
            NMonitoring::TDynamicCounters::TCounterPtr Allocated;
        } Counters;

        void UpdateAwaitCounters() {
            if (Counters.Await) {
                Counters.Await->Set(Await);
                Counters.AwaitOperations->Set(AwaitOperations);
            }
        }

        void UpdateAllocatedCounter() {
            if (Counters.Allocated) {
                Counters.Allocated->Set(Allocated);
            }
        }

        void Cleanup() {
            AllocatedTotal -= Allocated;
            Await = 0;
            Allocated = 0;
            AwaitOperations = 0;
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

     bool Suspend(TWaitInfo&& info) final {
        const auto& user = info.Request.GetUser();
        // Reuse the lookup context for insertion to avoid hashing a new user twice.
        THistoryMap::insert_ctx insertCtx = nullptr;
        auto userIt = AllocationsHistory.find(user, insertCtx);

        if (info.Request.GetCount() > 1U) {
            if (userIt != AllocationsHistory.end() && userIt->second.GetAwaitOperations() >= MaxOperationsPerUser) {
                return false;
            }
            if (LargeWaitList.size() >= MaxOperations) {
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
                    EnablePerUserMetrics ? Counters.get() : nullptr,
                    user));
        }

        userIt->second.Enqueue(info.Request.GetCount());
        (info.Request.GetCount() > 1U ? LargeWaitList : SmallWaitList).emplace_back(std::move(info), &userIt->second);
        return true;
    }

    std::vector<NActors::TActorId> Cleanup() final {
        std::vector<NActors::TActorId> senders;
        senders.reserve(SmallWaitList.size() + LargeWaitList.size());
        std::transform(SmallWaitList.cbegin(), SmallWaitList.cend(), std::back_inserter(senders), [](const TWaitInfo& info) { return info.Sender; });
        std::transform(LargeWaitList.cbegin(), LargeWaitList.cend(), std::back_inserter(senders), [](const TWaitInfo& info) { return info.Sender; });
        SmallWaitList.clear();
        LargeWaitList.clear();
        AllocationsHistory.clear();
        return senders;
    }

    size_t UpdateMetrics() final {
        if (Counters) {
            Counters->KnownUsers->Set(AllocationsHistory.size());
            Counters->AllocatedTotal->Set(AllocatedTotal);
            Counters->QueueSizeForSmall->Set(SmallWaitList.size());
            Counters->QueueSizeForLarge->Set(LargeWaitList.size());
            Counters->IntegralQueueSizeForLarge->Set(std::accumulate(LargeWaitList.cbegin(), LargeWaitList.cend(), 0ULL,
                [] (ui64 c, const TFullWaitInfo& info) { return c += info.Request.GetCount(); }
            ));
        }

        return SmallWaitList.size() + LargeWaitList.size();
    }

    void Process(size_t total, size_t count, const TProcessor& processor, const TInstant& now) final {
        const auto from = now - HistoryKeepingTime;
        EraseNodesIf(AllocationsHistory, [from](auto& item) {
            auto& userInfo = item.second;
            userInfo.ExpireAllocations(from);
            return userInfo.CanBeRemoved();
        });

        const auto sort = [](const TFullWaitInfo& lhs, const TFullWaitInfo& rhs) {
            return lhs.UserInfo->GetAllocated() < rhs.UserInfo->GetAllocated();
        };

        SmallWaitList.sort(sort);
        LargeWaitList.sort(sort);

        const auto work = [&processor, &now, &total, this] (size_t& quota, std::list<TFullWaitInfo>& list) {
            list.remove_if([&](const TFullWaitInfo& info) {
                const auto count = info.Request.GetCount();
                if (quota < count)
                    return false;

                if (EnableLimiter && (info.UserInfo->GetAllocated() + count) > LimiterNumerator * total / LimiterDenumerator) {
                    if (Counters) {
                        *Counters->UserLimited += 1;
                    }
                    return false;
                }

                if (processor(info)) {
                    info.UserInfo->Allocate(count, now);
                    quota -= count;
                    return true;
                }
                return false;
            });
        };

        const auto full = count;
        const auto half = full >> 1U;

        if (count -= half)
            work(count, SmallWaitList);

        if (KeepReserveForLiteralRequests)
            count = SmallWaitList.empty() && count + half >= total >> 2U ? std::min(count + half, full - (half >> 1U)) : half;
        else
            count += half;

        if (count > 1U)
            work(count, LargeWaitList);

        if (count && LargeWaitList.empty())
            work(count, SmallWaitList);
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

    i64 AllocatedTotal = 0;
    const TMyCounters::TPtr Counters;

    THistoryMap AllocationsHistory;
    std::list<TFullWaitInfo> SmallWaitList, LargeWaitList;

    bool EnableLimiter;
    ui32 LimiterNumerator;
    ui32 LimiterDenumerator;
};

}

IScheduler::TPtr IScheduler::Make(const NProto::TDqConfig::TScheduler& config, IMetricsRegistryPtr metricsRegistry) {
    return std::make_unique<TScheduler>(metricsRegistry, config);
}

}
