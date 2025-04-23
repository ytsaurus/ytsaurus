#include "distributed_throttler.h"
#include "distributed_throttler_proxy.h"
#include "config.h"

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/adjusted_exponential_moving_average.h>

#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/discovery_client/discovery_client.h>
#include <yt/yt/ytlib/discovery_client/member_client.h>

#include <yt/yt/library/numeric/binary_search.h>
#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NDistributedThrottler {

using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NConcurrency;
using namespace NApi::NNative;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TString AddressAttributeKey = "address";
static const TString RealmIdAttributeKey = "realm_id";
static const TString LeaderIdAttributeKey = "leader_id";
static const TString LocalThrottlersAttributeKey = "local_throttlers";
static const TString GlobalThrottlersAttributeKey = "global_throttlers";

////////////////////////////////////////////////////////////////////////////////

// Single member throttler usage.
struct TThrottlerLocalUsage
{
    double Rate = 0.0;
    double Limit = 0.0;
    i64 QueueTotalAmount = 0;
    TDuration EstimatedOverdraftDuration = TDuration::Zero();
};
// Throttler usage collected on the leader over all group members.
using TThrottlerGlobalUsage = TThrottlerUsage;

////////////////////////////////////////////////////////////////////////////////

using NYT::FromProto;

void FromProto(TThrottlerLocalUsage* throttlerLocalUsage, const NProto::TThrottlerData& throttlerData)
{
    throttlerLocalUsage->Rate = throttlerData.usage_rate();
    throttlerLocalUsage->Limit = throttlerData.limit();
    throttlerLocalUsage->QueueTotalAmount = throttlerData.queue_total_amount();
    throttlerLocalUsage->EstimatedOverdraftDuration = FromProto<TDuration>(throttlerData.estimated_overdraft_duration());
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TWrappedThrottler)

class TWrappedThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    TWrappedThrottler(
        TThrottlerId throttlerId,
        TDistributedThrottlerConfigPtr config,
        TThroughputThrottlerConfigPtr throttlerConfig,
        TDuration throttleRpcTimeout,
        TProfiler profiler)
        : Underlying_(CreateReconfigurableThroughputThrottler(throttlerConfig))
        , ThrottlerId_(std::move(throttlerId))
        , Config_(std::move(config))
        , ThrottlerConfig_(std::move(throttlerConfig))
        , ThrottleRpcTimeout_(throttleRpcTimeout)
        , Profiler_(profiler
            .WithTag("throttler_id", ThrottlerId_))
        , HistoricUsageAggregator_(Config_.Acquire()->AdjustedEmaHalflife)
    { }

    void SetDistributedThrottlerConfig(TDistributedThrottlerConfigPtr config)
    {
        auto guard = Guard(HistoricUsageAggregatorLock_);
        HistoricUsageAggregator_.SetHalflife(config->AdjustedEmaHalflife);
        Config_.Store(std::move(config));
    }

    double GetUsageRate()
    {
        auto guard = Guard(HistoricUsageAggregatorLock_);

        auto usage = HistoricUsageAggregator_.GetAverage();
        if (Initialized_) {
            Usage_.Update(usage);
        }
        return usage;
    }

    TThroughputThrottlerConfigPtr GetConfig() const override
    {
        return ThrottlerConfig_.Acquire();
    }

    TFuture<void> GetAvailableFuture() override
    {
        // This method is only called at Cypress Proxies. Throttler's mode is validated
        // in the dynamic config postprocessor there.
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        return Underlying_->GetAvailableFuture();
    }

    TFuture<void> Throttle(i64 amount) override
    {
        auto config = Config_.Acquire();

        if (config->Mode == EDistributedThrottlerMode::Precise) {
            if (auto leaderChannel = LeaderChannel_.Acquire()) {
                TDistributedThrottlerProxy proxy(leaderChannel);

                auto req = proxy.Throttle();
                req->SetTimeout(ThrottleRpcTimeout_);
                req->set_throttler_id(ThrottlerId_);
                req->set_amount(amount);

                return req->Invoke().As<void>();
            }
            // Either we are leader or we dont know the leader yet.
        }

        auto future = Underlying_->Throttle(amount);
        future.Subscribe(BIND([=, this, this_ = MakeStrong(this)] (const TError& error) {
            if (error.IsOK()) {
                UpdateHistoricUsage(amount);
            }
        }));
        return future;
    }

    bool TryAcquire(i64 amount) override
    {
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        auto result = Underlying_->TryAcquire(amount);
        if (result) {
            UpdateHistoricUsage(amount);
        }
        return result;
    }

    i64 TryAcquireAvailable(i64 amount) override
    {
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        auto result = Underlying_->TryAcquireAvailable(amount);
        if (result > 0) {
            UpdateHistoricUsage(result);
        }
        return result;
    }

    void Acquire(i64 amount) override
    {
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        UpdateHistoricUsage(amount);
        Underlying_->Acquire(amount);
    }

    void Release(i64 /*amount*/) override
    {
        YT_UNIMPLEMENTED();
    }

    bool IsOverdraft() override
    {
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        return Underlying_->IsOverdraft();
    }

    i64 GetQueueTotalAmount() const override
    {
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        auto queueTotalAmount = Underlying_->GetQueueTotalAmount();

        if (Initialized_) {
            QueueTotalAmount_.Update(queueTotalAmount);
        }

        return queueTotalAmount;
    }

    void Reconfigure(TThroughputThrottlerConfigPtr config) override
    {
        if (Config_.Acquire()->Mode == EDistributedThrottlerMode::Precise) {
            Underlying_->Reconfigure(std::move(config));
        } else {
            ThrottlerConfig_.Store(CloneYsonStruct(std::move(config)));
        }
    }

    void SetLimit(std::optional<double> limit) override
    {
        Underlying_->SetLimit(limit);

        if (Initialized_) {
            Limit_.Update(limit.value_or(-1));
        }
    }

    std::optional<double> GetLimit() const override
    {
        return Underlying_->GetLimit();
    }

    TDuration GetEstimatedOverdraftDuration() const override
    {
        auto duration = Underlying_->GetEstimatedOverdraftDuration();

        if (Initialized_) {
            EstimatedOverdraftDuration_.Update(duration);
        }

        return duration;
    }

    void SetLeaderChannel(IChannelPtr leaderChannel)
    {
        LeaderChannel_.Store(leaderChannel);
    }

    i64 GetAvailable() const override
    {
        YT_VERIFY(Config_.Acquire()->Mode != EDistributedThrottlerMode::Precise);

        return Underlying_->GetAvailable();
    }

    operator TThrottlerLocalUsage()
    {
        return TThrottlerLocalUsage{
            .Rate = GetUsageRate(),
            .Limit = GetLimit().value_or(0),
            .QueueTotalAmount = GetQueueTotalAmount(),
            .EstimatedOverdraftDuration = GetEstimatedOverdraftDuration(),
        };
    }

private:
    const IReconfigurableThroughputThrottlerPtr Underlying_;
    const TThrottlerId ThrottlerId_;

    TAtomicIntrusivePtr<TDistributedThrottlerConfig> Config_;
    TAtomicIntrusivePtr<TThroughputThrottlerConfig> ThrottlerConfig_;

    const TDuration ThrottleRpcTimeout_;

    TAtomicIntrusivePtr<IChannel> LeaderChannel_;

    TProfiler Profiler_;
    TGauge Limit_;
    TGauge Usage_;
    TGauge QueueTotalAmount_;
    TTimeGauge EstimatedOverdraftDuration_;
    std::atomic<bool> Initialized_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, HistoricUsageAggregatorLock_);
    TAverageAdjustedExponentialMovingAverage HistoricUsageAggregator_;

    void Initialize()
    {
        if (Initialized_) {
            return;
        }

        YT_ASSERT_SPINLOCK_AFFINITY(HistoricUsageAggregatorLock_);

        Initialized_ = true;

        Limit_ = Profiler_.Gauge("/limit");
        Usage_ = Profiler_.Gauge("/usage");
        QueueTotalAmount_ = Profiler_.Gauge("/queue_total_amount");
        EstimatedOverdraftDuration_ = Profiler_.TimeGauge("/estimated_overdraft_duration");

        Limit_.Update(ThrottlerConfig_.Acquire()->Limit.value_or(-1));
        QueueTotalAmount_.Update(0);
        EstimatedOverdraftDuration_.Update(TDuration::Zero());
    }

    void UpdateHistoricUsage(i64 amount)
    {
        auto guard = Guard(HistoricUsageAggregatorLock_);
        HistoricUsageAggregator_.UpdateAt(TInstant::Now(), amount);
        if (amount > 0) {
            Initialize();
        }
        if (Initialized_) {
            Usage_.Update(HistoricUsageAggregator_.GetAverage());
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TWrappedThrottler)

////////////////////////////////////////////////////////////////////////////////

using NYT::ToProto;

void ToProto(NProto::TThrottlerData* throttlerData, TWrappedThrottlerPtr throttler)
{
    throttlerData->set_usage_rate(throttler->GetUsageRate());
    throttlerData->set_limit(throttler->GetLimit().value_or(0));
    throttlerData->set_queue_total_amount(throttler->GetQueueTotalAmount());
    throttlerData->set_estimated_overdraft_duration(ToProto(throttler->GetEstimatedOverdraftDuration()));
}

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TThrottlers)

struct TThrottlers final
{
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock);
    THashMap<TThrottlerId, TWeakPtr<TWrappedThrottler>> Throttlers;
};


DEFINE_REFCOUNTED_TYPE(TThrottlers)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedThrottlerService)

class TDistributedThrottlerService
    : public TServiceBase
{
public:
    TDistributedThrottlerService(
        IServerPtr rpcServer,
        IInvokerPtr invoker,
        IDiscoveryClientPtr discoveryClient,
        TGroupId groupId,
        TDistributedThrottlerConfigPtr config,
        TRealmId realmId,
        TThrottlersPtr throttlers,
        NLogging::TLogger logger,
        IAuthenticatorPtr authenticator,
        int shardCount = 16)
        : TServiceBase(
            invoker,
            TDistributedThrottlerProxy::GetDescriptor(),
            logger,
            TServiceOptions{
                .RealmId = realmId,
                .Authenticator = authenticator,
            })
        , RpcServer_(std::move(rpcServer))
        , DiscoveryClient_(std::move(discoveryClient))
        , GroupId_(std::move(groupId))
        , UpdatePeriodicExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TDistributedThrottlerService::UpdateLimits, MakeWeak(this)),
            config->LimitUpdatePeriod))
        , Throttlers_(std::move(throttlers))
        , Logger(std::move(logger))
        , ShardCount_(shardCount)
        , Config_(std::move(config))
        , MemberShards_(ShardCount_)
        , ThrottlerShards_(ShardCount_)
    {
        YT_VERIFY(ShardCount_ > 0);
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Throttle)
            .SetCancelable(true)
            .SetConcurrencyLimit(10000)
            .SetQueueSizeLimit(20000));
    }

    void Initialize()
    {
        if (Active_) {
            return;
        }

        RpcServer_->RegisterService(this);
        UpdatePeriodicExecutor_->Start();
        Active_ = true;
    }

    void Finalize()
    {
        if (!Active_) {
            return;
        }

        YT_UNUSED_FUTURE(UpdatePeriodicExecutor_->Stop());
        RpcServer_->UnregisterService(this);
        Active_ = false;
    }

    void Reconfigure(const TDistributedThrottlerConfigPtr& config)
    {
        auto oldConfig = Config_.Acquire();

        if (oldConfig->LimitUpdatePeriod != config->LimitUpdatePeriod) {
            auto guard = Guard(ReconfigurationLock_);
            Config_.Store(config);
            UpdatePeriodicExecutor_->SetPeriod(config->LimitUpdatePeriod);
        }
    }

    void SetTotalLimit(const TThrottlerId& throttlerId, std::optional<double> newLimit)
    {
        auto* shard = GetThrottlerShard(throttlerId);

        auto guard = WriterGuard(shard->TotalLimitsLock);
        auto& limit = shard->ThrottlerIdToTotalLimit[throttlerId];
        if (auto oldLimit = limit; newLimit != oldLimit) {
            YT_LOG_DEBUG("Changing throttler total limit (ThrottlerId: %v, Limit: %v -> %v)",
                throttlerId,
                oldLimit,
                newLimit);
            limit = newLimit;
        }
    }

    void UpdateMemberThrottlersLocalUsage(const TMemberId& memberId, THashMap<TThrottlerId, TThrottlerLocalUsage> throttlerIdToLocalUsage)
    {
        auto config = Config_.Acquire();

        std::vector<std::vector<TThrottlerId>> throttlerIdsByShard(ShardCount_);
        for (const auto& [throttlerId, _] : throttlerIdToLocalUsage) {
            throttlerIdsByShard[GetShardIndex(throttlerId)].push_back(throttlerId);
        }

        auto now = TInstant::Now();
        for (int i = 0; i < ShardCount_; ++i) {
            if (throttlerIdsByShard[i].empty()) {
                continue;
            }

            auto& shard = ThrottlerShards_[i];
            auto guard = WriterGuard(shard.LastUpdateTimeLock);
            for (const auto& throttlerId : throttlerIdsByShard[i]) {
                shard.ThrottlerIdToLastUpdateTime[throttlerId] = now;
            }
        }

        {
            auto* shard = GetMemberShard(memberId);

            auto guard = WriterGuard(shard->UsageRatesLock);

            auto& memberThrottlersLocalUsage = shard->MemberIdToThrottlersLocalUsage[memberId];
            for (const auto& [throttlerId, localUsage] : throttlerIdToLocalUsage) {
                memberThrottlersLocalUsage[throttlerId] = localUsage;
            }
        }
    }

    THashMap<TThrottlerId, std::optional<double>> GetMemberLimits(const TMemberId& memberId, const std::vector<TThrottlerId>& throttlerIds)
    {
        auto config = Config_.Acquire();

        std::vector<std::vector<TThrottlerId>> throttlerIdsByShard(ShardCount_);
        for (const auto& throttlerId : throttlerIds) {
            throttlerIdsByShard[GetShardIndex(throttlerId)].push_back(throttlerId);
        }

        THashMap<TThrottlerId, std::optional<double>> result;
        for (int i = 0; i < ShardCount_; ++i) {
            if (throttlerIdsByShard[i].empty()) {
                continue;
            }

            auto& throttlerShard = ThrottlerShards_[i];
            auto totalLimitsGuard = ReaderGuard(throttlerShard.TotalLimitsLock);
            for (const auto& throttlerId : throttlerIdsByShard[i]) {
                auto totalLimitIt = throttlerShard.ThrottlerIdToTotalLimit.find(throttlerId);
                if (totalLimitIt == throttlerShard.ThrottlerIdToTotalLimit.end()) {
                    YT_LOG_DEBUG("There is no total limit for throttler (ThrottlerId: %v)", throttlerId);
                    continue;
                }

                auto optionalTotalLimit = totalLimitIt->second;
                if (!optionalTotalLimit) {
                    YT_VERIFY(result.emplace(throttlerId, std::nullopt).second);
                }
            }

            auto fillLimits = [&] (const THashMap<TThrottlerId, double>& throttlerIdToLimits) {
                for (const auto& throttlerId : throttlerIdsByShard[i]) {
                    if (result.contains(throttlerId)) {
                        continue;
                    }
                    auto limitIt = throttlerIdToLimits.find(throttlerId);
                    if (limitIt == throttlerIdToLimits.end()) {
                        YT_LOG_DEBUG("There is no total limit for throttler (ThrottlerId: %v)", throttlerId);
                    } else {
                        YT_VERIFY(result.emplace(throttlerId, limitIt->second).second);
                    }
                }
            };

            if (config->Mode == EDistributedThrottlerMode::Uniform) {
                auto guard = ReaderGuard(throttlerShard.UniformLimitLock);
                fillLimits(throttlerShard.ThrottlerIdToUniformLimit);
            } else {
                auto* shard = GetMemberShard(memberId);

                auto guard = ReaderGuard(shard->LimitsLock);
                auto memberIt = shard->MemberIdToLimit.find(memberId);
                if (memberIt != shard->MemberIdToLimit.end()) {
                    fillLimits(memberIt->second);
                }
            }
        }

        return result;
    }

    TIntrusivePtr<const TThrottlerToGlobalUsage> GetThrottlerToGlobalUsage() const
    {
        return ThrottlerToGlobalUsage_.Acquire();
    }

private:
    const IServerPtr RpcServer_;
    const IDiscoveryClientPtr DiscoveryClient_;
    const TGroupId GroupId_;
    const TPeriodicExecutorPtr UpdatePeriodicExecutor_;
    const TThrottlersPtr Throttlers_;
    const NLogging::TLogger Logger;
    const int ShardCount_;

    TAtomicIntrusivePtr<const TThrottlerToGlobalUsage> ThrottlerToGlobalUsage_;
    bool Active_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReconfigurationLock_);
    TAtomicIntrusivePtr<TDistributedThrottlerConfig> Config_;

    struct TMemberShard
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, LimitsLock);
        THashMap<TMemberId, THashMap<TThrottlerId, double>> MemberIdToLimit;

        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, UsageRatesLock);
        THashMap<TMemberId, THashMap<TThrottlerId, TThrottlerLocalUsage>> MemberIdToThrottlersLocalUsage;
    };
    std::vector<TMemberShard> MemberShards_;

    struct TThrottlerShard
    {
        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, TotalLimitsLock);
        THashMap<TThrottlerId, std::optional<double>> ThrottlerIdToTotalLimit;

        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, UniformLimitLock);
        THashMap<TThrottlerId, double> ThrottlerIdToUniformLimit;

        YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, LastUpdateTimeLock);
        THashMap<TThrottlerId, TInstant> ThrottlerIdToLastUpdateTime;
    };
    std::vector<TThrottlerShard> ThrottlerShards_;

    DECLARE_RPC_SERVICE_METHOD(NDistributedThrottler::NProto, Heartbeat)
    {
        auto config = Config_.Acquire();

        if (config->Mode == EDistributedThrottlerMode::Precise) {
            THROW_ERROR_EXCEPTION(
                NDistributedThrottler::EErrorCode::UnexpectedThrottlerMode,
                "Cannot handle heartbeat request in %v mode",
                config->Mode);
        }

        const auto& memberId = request->member_id();

        context->SetRequestInfo("MemberId: %v, ThrottlerCount: %v",
            memberId,
            request->throttlers().size());

        THashMap<TThrottlerId, TThrottlerLocalUsage> throttlerIdToLocalUsage;
        throttlerIdToLocalUsage.reserve(request->throttlers().size());
        for (const auto& throttler : request->throttlers()) {
            const auto& throttlerId = throttler.id();
            auto localUsage = FromProto<TThrottlerLocalUsage>(throttler);
            EmplaceOrCrash(throttlerIdToLocalUsage, throttlerId, std::move(localUsage));
        }

        auto limits = GetMemberLimits(memberId, GetKeys(throttlerIdToLocalUsage));
        for (const auto& [throttlerId, limit] : limits) {
            auto* result = response->add_throttlers();
            result->set_id(throttlerId);
            if (limit) {
                result->set_limit(*limit);
            }
        }
        UpdateMemberThrottlersLocalUsage(memberId, std::move(throttlerIdToLocalUsage));

        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NDistributedThrottler::NProto, Throttle)
    {
        auto config = Config_.Acquire();

        if (config->Mode != EDistributedThrottlerMode::Precise) {
            THROW_ERROR_EXCEPTION(
                NDistributedThrottler::EErrorCode::UnexpectedThrottlerMode,
                "Cannot handle throttle request in %v mode",
                config->Mode);
        }

        const auto& throttlerId = request->throttler_id();
        auto amount = request->amount();

        context->SetRequestInfo("ThrottlerId: %v, Amount: %v",
            throttlerId,
            amount);

        Throttle(throttlerId, amount).Subscribe(BIND([=] (const TError& error) {
            context->Reply(error);
        }));
    }

    IReconfigurableThroughputThrottlerPtr FindThrottler(const TThrottlerId& throttlerId)
    {
        auto guard = ReaderGuard(Throttlers_->Lock);

        auto it = Throttlers_->Throttlers.find(throttlerId);
        if (it == Throttlers_->Throttlers.end()) {
            return nullptr;
        }
        return it->second.Lock();
    }

    TFuture<void> Throttle(const TThrottlerId& throttlerId, i64 amount)
    {
        auto throttler = FindThrottler(throttlerId);
        if (!throttler) {
            return MakeFuture(TError(NDistributedThrottler::EErrorCode::NoSuchThrottler, "No such throttler %Qv", throttlerId));
        }

        return throttler->Throttle(amount);
    }

    int GetShardIndex(const TMemberId& memberId)
    {
        return THash<TMemberId>()(memberId) % ShardCount_;
    }

    TMemberShard* GetMemberShard(const TMemberId& memberId)
    {
        return &MemberShards_[GetShardIndex(memberId)];
    }

    TThrottlerShard* GetThrottlerShard(const TMemberId& memberId)
    {
        return &ThrottlerShards_[GetShardIndex(memberId)];
    }

    void UpdateUniformLimitDistribution()
    {
        auto amountRspOrError = WaitFor(DiscoveryClient_->GetGroupMeta(GroupId_));
        if (!amountRspOrError.IsOK()) {
            YT_LOG_WARNING(amountRspOrError, "Error updating throttler limits");
            return;
        }

        auto totalCount = amountRspOrError.Value().MemberCount;
        if (totalCount == 0) {
            YT_LOG_WARNING("No members in current group");
            return;
        }

        for (auto& shard : ThrottlerShards_) {
            THashMap<TThrottlerId, double> throttlerIdToUniformLimit;
            {
                auto guard = ReaderGuard(shard.TotalLimitsLock);
                for (const auto& [throttlerId, optionalTotalLimit] : shard.ThrottlerIdToTotalLimit) {
                    if (!optionalTotalLimit) {
                        continue;
                    }

                    auto uniformLimit = std::max<double>(0, *optionalTotalLimit / totalCount);
                    YT_VERIFY(throttlerIdToUniformLimit.emplace(throttlerId, uniformLimit).second);
                    YT_LOG_TRACE("Uniform distribution limit updated (ThrottlerId: %v, UniformLimit: %v)",
                        throttlerId,
                        uniformLimit);
                }
            }

            {
                auto guard = WriterGuard(shard.UniformLimitLock);
                shard.ThrottlerIdToUniformLimit.swap(throttlerIdToUniformLimit);
            }
        }
    }

    void UpdateLimits()
    {
        ForgetDeadThrottlers();

        auto config = Config_.Acquire();

        if (config->Mode == EDistributedThrottlerMode::Precise) {
            return;
        }

        if (config->Mode == EDistributedThrottlerMode::Uniform) {
            UpdateUniformLimitDistribution();
            return;
        }

        // Reserve space for new throttler global data.
        int throttlerCount = 0;
        for (const auto& throttlerShard : ThrottlerShards_) {
            auto guard = ReaderGuard(throttlerShard.TotalLimitsLock);
            throttlerCount += throttlerShard.ThrottlerIdToTotalLimit.size();
        }
        auto newThrottlerToGlobalUsage = New<TThrottlerToGlobalUsage>();
        newThrottlerToGlobalUsage->reserve(throttlerCount);

        // Calculate new member limits.
        std::vector<THashMap<TMemberId, THashMap<TThrottlerId, double>>> memberIdToLimit(ShardCount_);
        for (auto& throttlerShard : ThrottlerShards_) {
            THashMap<TThrottlerId, std::optional<double>> throttlerIdToTotalLimit;
            {
                auto guard = ReaderGuard(throttlerShard.TotalLimitsLock);
                throttlerIdToTotalLimit = throttlerShard.ThrottlerIdToTotalLimit;
            }

            THashMap<TThrottlerId, double> throttlerIdToTotalUsageRate;
            THashMap<TThrottlerId, THashMap<TMemberId, double>> throttlerIdToUsageRates;
            int memberCount = 0;

            for (auto& shard : MemberShards_) {
                auto guard = ReaderGuard(shard.UsageRatesLock);
                memberCount += shard.MemberIdToThrottlersLocalUsage.size();
                for (const auto& [memberId, throttlers] : shard.MemberIdToThrottlersLocalUsage) {
                    for (const auto& [throttlerId, totalLimit] : throttlerIdToTotalLimit) {
                        auto throttlerIt = throttlers.find(throttlerId);
                        if (throttlerIt == throttlers.end()) {
                            YT_LOG_DEBUG("Member does not know about throttler (MemberId: %v, ThrottlerId: %v)",
                                memberId,
                                throttlerId);
                            continue;
                        }

                        const auto& throttlerLocalUsage = throttlerIt->second;
                        throttlerIdToTotalUsageRate[throttlerId] += throttlerLocalUsage.Rate;
                        throttlerIdToUsageRates[throttlerId].emplace(memberId, throttlerLocalUsage.Rate);

                        auto& throttlerGlobalUsage = (*newThrottlerToGlobalUsage)[throttlerId];
                        throttlerGlobalUsage.Rate += throttlerLocalUsage.Rate;
                        throttlerGlobalUsage.QueueTotalAmount += throttlerLocalUsage.QueueTotalAmount;
                        throttlerGlobalUsage.MaxEstimatedOverdraftDuration = std::max(
                            throttlerGlobalUsage.MaxEstimatedOverdraftDuration,
                            throttlerLocalUsage.EstimatedOverdraftDuration);
                        throttlerGlobalUsage.MinEstimatedOverdraftDuration = std::min(
                            throttlerGlobalUsage.MinEstimatedOverdraftDuration,
                            throttlerLocalUsage.EstimatedOverdraftDuration);
                    }
                }
            }

            for (const auto& [throttlerId, totalUsageRate] : throttlerIdToTotalUsageRate) {
                auto optionalTotalLimit = GetOrCrash(throttlerIdToTotalLimit, throttlerId);
                if (!optionalTotalLimit) {
                    continue;
                }
                auto totalLimit = *optionalTotalLimit;

                auto defaultLimit = FloatingPointInverseLowerBound(0, totalLimit, [&, &throttlerId = throttlerId] (double value) {
                    double total = 0;
                    for (const auto& [memberId, usageRate] : throttlerIdToUsageRates[throttlerId]) {
                        total += Min(value, usageRate);
                    }
                    return total <= totalLimit;
                });

                auto extraLimit = (config->ExtraLimitRatio * totalLimit + Max<double>(0, totalLimit - totalUsageRate)) / memberCount;

                for (const auto& [memberId, usageRate] : GetOrCrash(throttlerIdToUsageRates, throttlerId)) {
                    auto newLimit = Min(usageRate, defaultLimit) + extraLimit;
                    YT_LOG_TRACE(
                        "Updating throttler limit (MemberId: %v, ThrottlerId: %v, UsageRate: %v, NewLimit: %v, ExtraLimit: %v)",
                        memberId,
                        throttlerId,
                        usageRate,
                        newLimit,
                        extraLimit);
                    YT_VERIFY(memberIdToLimit[GetShardIndex(memberId)][memberId].emplace(throttlerId, newLimit).second);
                    (*newThrottlerToGlobalUsage)[throttlerId].Limit += newLimit;
                }
            }
        }

        // Update member limits.
        {
            for (int i = 0; i < ShardCount_; ++i) {
                auto& shard = MemberShards_[i];
                auto guard = WriterGuard(shard.LimitsLock);
                shard.MemberIdToLimit.swap(memberIdToLimit[i]);
            }
        }

        // Update throttlers global data.
        ThrottlerToGlobalUsage_.Store(newThrottlerToGlobalUsage.Get());
    }

    void ForgetDeadThrottlers()
    {
        auto config = Config_.Acquire();

        for (auto& throttlerShard : ThrottlerShards_) {
            std::vector<TThrottlerId> deadThrottlersIds;

            {
                auto now = TInstant::Now();
                auto guard = ReaderGuard(throttlerShard.LastUpdateTimeLock);
                for (const auto& [throttlerId, lastUpdateTime] : throttlerShard.ThrottlerIdToLastUpdateTime) {
                    if (lastUpdateTime + config->ThrottlerExpirationTime < now) {
                        deadThrottlersIds.push_back(throttlerId);
                    }
                }
            }

            if (deadThrottlersIds.empty()) {
                continue;
            }

            {
                auto guard = WriterGuard(throttlerShard.TotalLimitsLock);
                for (const auto& deadThrottlerId : deadThrottlersIds) {
                    throttlerShard.ThrottlerIdToTotalLimit.erase(deadThrottlerId);
                }
            }

            {
                auto guard = WriterGuard(throttlerShard.UniformLimitLock);
                for (const auto& deadThrottlerId : deadThrottlersIds) {
                    throttlerShard.ThrottlerIdToUniformLimit.erase(deadThrottlerId);
                }
            }

            for (auto& memberShard : MemberShards_) {
                auto guard = WriterGuard(memberShard.LimitsLock);
                for (auto& [memberId, throttlerIdToLimit] : memberShard.MemberIdToLimit) {
                    for (const auto& deadThrottlerId : deadThrottlersIds) {
                        throttlerIdToLimit.erase(deadThrottlerId);
                    }
                }
            }

            for (auto& memberShard : MemberShards_) {
                auto guard = WriterGuard(memberShard.UsageRatesLock);
                for (auto& [memberId, throttlerIdToLocalUsage] : memberShard.MemberIdToThrottlersLocalUsage) {
                    for (const auto& deadThrottlerId : deadThrottlersIds) {
                        throttlerIdToLocalUsage.erase(deadThrottlerId);
                    }
                }
            }

            {
                auto guard = WriterGuard(throttlerShard.LastUpdateTimeLock);
                for (const auto& deadThrottlerId : deadThrottlersIds) {
                    throttlerShard.ThrottlerIdToLastUpdateTime.erase(deadThrottlerId);
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerService)

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerFactory
    : public IDistributedThrottlerFactory
{
public:
    TDistributedThrottlerFactory(
        TDistributedThrottlerConfigPtr config,
        IChannelFactoryPtr channelFactory,
        IConnectionPtr connection,
        IInvokerPtr invoker,
        TGroupId groupId,
        TMemberId memberId,
        IServerPtr rpcServer,
        TString address,
        const NLogging::TLogger& logger,
        IAuthenticatorPtr authenticator,
        TProfiler profiler)
        : ChannelFactory_(std::move(channelFactory))
        , Connection_(std::move(connection))
        , GroupId_(std::move(groupId))
        , MemberId_(std::move(memberId))
        , MemberClient_(Connection_->CreateMemberClient(
            config->MemberClient,
            ChannelFactory_,
            invoker,
            MemberId_,
            GroupId_))
        , DiscoveryClient_(Connection_->CreateDiscoveryClient(
            config->DiscoveryClient,
            ChannelFactory_))
        , UpdateLimitsExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TDistributedThrottlerFactory::UpdateLimits, MakeWeak(this)),
            config->LimitUpdatePeriod))
        , UpdateLeaderExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TDistributedThrottlerFactory::UpdateLeader, MakeWeak(this)),
            config->LeaderUpdatePeriod))
        , UpdateThrottlersAttributesExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TDistributedThrottlerFactory::UpdateThrottlersAttributes, MakeWeak(this)),
            config->ThrottlersAttributesUpdatePeriod))
        , RealmId_(TGuid::Create())
        , Logger(logger.WithTag("SelfMemberId: %v, GroupId: %v, RealmId: %v",
            MemberId_,
            GroupId_,
            RealmId_))
        , Profiler_(std::move(profiler))
        , Config_(std::move(config))
        , DistributedThrottlerService_(New<TDistributedThrottlerService>(
            std::move(rpcServer),
            std::move(invoker),
            DiscoveryClient_,
            GroupId_,
            Config_.Acquire(),
            RealmId_,
            Throttlers_,
            Logger,
            std::move(authenticator)))
    {
        auto* attributes = MemberClient_->GetAttributes();
        attributes->Set(RealmIdAttributeKey, RealmId_);
        attributes->Set(AddressAttributeKey, address);

        MemberClient_->SetPriority(TInstant::Now().Seconds());
        UpdateLimitsExecutor_->Start();
        UpdateLeaderExecutor_->Start();
        UpdateThrottlersAttributesExecutor_->Start();
    }

    ~TDistributedThrottlerFactory()
    {
        YT_UNUSED_FUTURE(MemberClient_->Stop());
        DistributedThrottlerService_->Finalize();
    }

    IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(
        const TThrottlerId& throttlerId,
        TThroughputThrottlerConfigPtr throttlerConfig,
        TDuration throttleRpcTimeout) override
    {
        auto updateUpdateQueue = [&, this] (const TWrappedThrottlerPtr& throttler) {
            auto queueGuard = Guard(UpdateQueueLock_);
            YT_VERIFY(throttler);
            UpdateQueue_.emplace(throttlerId, MakeWeak(throttler));
            UnreportedThrottlers_.insert(throttlerId);
        };

        auto findThrottler = [&] (const TThrottlerId& throttlerId) -> TWrappedThrottlerPtr {
            auto it = Throttlers_->Throttlers.find(throttlerId);
            if (it == Throttlers_->Throttlers.end()) {
                return nullptr;
            }

            auto throttler = it->second.Lock();
            if (!throttler) {
                return nullptr;
            }
            return throttler;
        };

        auto onThrottlerFound = [&] (const TWrappedThrottlerPtr& throttler) {
            DistributedThrottlerService_->SetTotalLimit(throttlerId, throttlerConfig->Limit);
            throttler->Reconfigure(std::move(throttlerConfig));
            updateUpdateQueue(throttler);
        };

        TWrappedThrottlerPtr wrappedThrottler;

        // Fast path.
        {
            auto guard = ReaderGuard(Throttlers_->Lock);
            wrappedThrottler = findThrottler(throttlerId);
        }
        if (wrappedThrottler) {
            onThrottlerFound(wrappedThrottler);
            return wrappedThrottler;
        }

        // Slow path.
        IChannelPtr leaderChannel;
        {
            auto readerGuard = ReaderGuard(Lock_);
            // NB: Could be null.
            leaderChannel = LeaderChannel_;
        }

        {
            auto guard = WriterGuard(Throttlers_->Lock);
            wrappedThrottler = findThrottler(throttlerId);
            if (wrappedThrottler) {
                guard.Release();
                onThrottlerFound(wrappedThrottler);
                return wrappedThrottler;
            }

            DistributedThrottlerService_->SetTotalLimit(throttlerId, throttlerConfig->Limit);
            wrappedThrottler = New<TWrappedThrottler>(
                throttlerId,
                Config_.Acquire(),
                std::move(throttlerConfig),
                throttleRpcTimeout,
                Profiler_);
            wrappedThrottler->SetLeaderChannel(leaderChannel);

            auto wasEmpty = Throttlers_->Throttlers.empty();
            Throttlers_->Throttlers[throttlerId] = std::move(wrappedThrottler);

            if (wasEmpty) {
                Start();
            }

            YT_LOG_DEBUG("Distributed throttler created (ThrottlerId: %v)", throttlerId);
        }

        updateUpdateQueue(wrappedThrottler);
        return wrappedThrottler;
    }

    void Reconfigure(TDistributedThrottlerConfigPtr config) override
    {
        MemberClient_->Reconfigure(config->MemberClient);
        DiscoveryClient_->Reconfigure(config->DiscoveryClient);

        auto oldConfig = Config_.Acquire();

        if (oldConfig->LimitUpdatePeriod != config->LimitUpdatePeriod) {
            UpdateLimitsExecutor_->SetPeriod(config->LimitUpdatePeriod);
        }
        if (oldConfig->LeaderUpdatePeriod != config->LeaderUpdatePeriod) {
            UpdateLeaderExecutor_->SetPeriod(config->LeaderUpdatePeriod);
        }
        if (oldConfig->ThrottlersAttributesUpdatePeriod != config->ThrottlersAttributesUpdatePeriod) {
            UpdateThrottlersAttributesExecutor_->SetPeriod(config->ThrottlersAttributesUpdatePeriod);
        }

        DistributedThrottlerService_->Reconfigure(config);

        {
            auto guard = ReaderGuard(Throttlers_->Lock);
            for (const auto& [throttlerId, weakThrottler] : Throttlers_->Throttlers) {
                auto throttler = weakThrottler.Lock();
                if (!throttler) {
                    continue;
                }
                throttler->SetDistributedThrottlerConfig(config);
            }
        }

        Config_.Store(std::move(config));
    }

    TIntrusivePtr<const TThrottlerToGlobalUsage> TryGetThrottlerToGlobalUsage() const override
    {
        auto guard = ReaderGuard(Lock_);
        if (MemberId_ == LeaderId_) {
            return DistributedThrottlerService_->GetThrottlerToGlobalUsage();
        }

        return nullptr;
    }

private:
    const IChannelFactoryPtr ChannelFactory_;
    const IConnectionPtr Connection_;
    const TGroupId GroupId_;
    const TMemberId MemberId_;
    const IMemberClientPtr MemberClient_;
    const IDiscoveryClientPtr DiscoveryClient_;
    const TPeriodicExecutorPtr UpdateLimitsExecutor_;
    const TPeriodicExecutorPtr UpdateLeaderExecutor_;
    const TPeriodicExecutorPtr UpdateThrottlersAttributesExecutor_;
    const TRealmId RealmId_;

    const NLogging::TLogger Logger;
    TProfiler Profiler_;

    TAtomicIntrusivePtr<TDistributedThrottlerConfig> Config_;

    const TThrottlersPtr Throttlers_ = New<TThrottlers>();
    const TDistributedThrottlerServicePtr DistributedThrottlerService_;

    std::atomic<bool> Active_ = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    std::optional<TMemberId> LeaderId_;
    IChannelPtr LeaderChannel_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, UpdateQueueLock_);
    TRingQueue<std::pair<TThrottlerId, TWeakPtr<TWrappedThrottler>>> UpdateQueue_;
    THashSet<TThrottlerId> UnreportedThrottlers_;

    void Start()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Throttlers_->Lock);

        YT_UNUSED_FUTURE(MemberClient_->Start());
        Active_ = true;
    }

    void Stop()
    {
        YT_ASSERT_SPINLOCK_AFFINITY(Throttlers_->Lock);

        Active_ = false;
        YT_UNUSED_FUTURE(MemberClient_->Stop());
    }

    void UpdateLimits()
    {
        if (!Active_) {
            return;
        }

        auto config = Config_.Acquire();
        if (config->Mode == EDistributedThrottlerMode::Precise) {
            return;
        }

        IChannelPtr leaderChannel;
        TMemberId leaderId;
        {
            auto guard = ReaderGuard(Lock_);
            if (!LeaderId_) {
                YT_LOG_DEBUG("Unable to update throttler limit: no active leader");
                UpdateLeaderExecutor_->ScheduleOutOfBand();
                return;
            }
            leaderId = *LeaderId_;
            leaderChannel = LeaderChannel_;
        }

        THashMap<TThrottlerId, TWrappedThrottlerPtr> throttlers;
        std::vector<TThrottlerId> deadThrottlerIds;
        std::vector<std::pair<TThrottlerId, TWrappedThrottlerPtr>> skippedThrottlers;

        int heartbeatThrottlerCountLimit = config->HeartbeatThrottlerCountLimit;
        int skipUnusedThrottlersCountLimit = config->SkipUnusedThrottlersCountLimit;

        {
            auto guard = Guard(UpdateQueueLock_);
            while (std::ssize(throttlers) < heartbeatThrottlerCountLimit &&
                   std::ssize(skippedThrottlers) < skipUnusedThrottlersCountLimit)
            {
                if (UpdateQueue_.empty()) {
                    break;
                }

                auto [throttlerId, weakThrottler] = std::move(UpdateQueue_.front());
                UpdateQueue_.pop();

                if (auto throttler = weakThrottler.Lock()) {
                    if (throttler->GetUsageRate() > 0 || UnreportedThrottlers_.contains(throttlerId)) {
                        UnreportedThrottlers_.erase(throttlerId);
                        throttlers.emplace(std::move(throttlerId), std::move(throttler));
                    } else {
                        skippedThrottlers.emplace_back(std::move(throttlerId), std::move(throttler));
                    }
                } else {
                    deadThrottlerIds.push_back(std::move(throttlerId));
                }
            }
        }

        if (!deadThrottlerIds.empty()) {
            auto guard = WriterGuard(Throttlers_->Lock);
            for (const auto& throttlerId : deadThrottlerIds) {
                auto it = Throttlers_->Throttlers.find(throttlerId);
                if (it == Throttlers_->Throttlers.end()) {
                    continue;
                }
                auto throttler = it->second.Lock();
                if (throttler) {
                    continue;
                }
                Throttlers_->Throttlers.erase(it);
            }

            if (Throttlers_->Throttlers.empty()) {
                Stop();
                return;
            }
        }

        if (leaderId == MemberId_) {
            UpdateLimitsAtLeader(throttlers);
        } else {
            UpdateLimitsAtFollower(std::move(leaderId), std::move(leaderChannel), throttlers);
        }

        {
            auto guard = Guard(UpdateQueueLock_);
            for (auto& [throttlerId, throttler] : throttlers) {
                UpdateQueue_.emplace(std::move(throttlerId), std::move(throttler));
            }

            for (auto& [throttlerId, throttler] : skippedThrottlers) {
                UpdateQueue_.emplace(std::move(throttlerId), std::move(throttler));
            }
        }
    }

    void UpdateLimitsAtLeader(const THashMap<TThrottlerId, TWrappedThrottlerPtr>& throttlers)
    {
        THashMap<TThrottlerId, TThrottlerLocalUsage> throttlerIdToLocalUsage;
        for (const auto& [throttlerId, throttler] : throttlers) {
            auto config = throttler->GetConfig();
            DistributedThrottlerService_->SetTotalLimit(throttlerId, config->Limit);

            auto localUsage = static_cast<TThrottlerLocalUsage>(*throttler);
            EmplaceOrCrash(throttlerIdToLocalUsage, throttlerId, std::move(localUsage));
        }

        auto limits = DistributedThrottlerService_->GetMemberLimits(MemberId_, GetKeys(throttlerIdToLocalUsage));
        for (const auto& [throttlerId, limit] : limits) {
            const auto& throttler = GetOrCrash(throttlers, throttlerId);
            throttler->SetLimit(limit);
            YT_LOG_TRACE("Throttler limit updated (ThrottlerId: %v, Limit: %v)",
                throttlerId,
                limit);
        }
        DistributedThrottlerService_->UpdateMemberThrottlersLocalUsage(MemberId_, std::move(throttlerIdToLocalUsage));
    }

    void UpdateLimitsAtFollower(
        TMemberId leaderId,
        IChannelPtr leaderChannel,
        const THashMap<TThrottlerId, TWrappedThrottlerPtr>& throttlers)
    {
        auto config = Config_.Acquire();

        TDistributedThrottlerProxy proxy(std::move(leaderChannel));

        auto req = proxy.Heartbeat();
        req->SetTimeout(config->ControlRpcTimeout);
        req->set_member_id(MemberId_);

        for (const auto& [throttlerId, throttler] : throttlers) {
            auto* protoThrottler = req->add_throttlers();
            protoThrottler->set_id(throttlerId);
            ToProto(protoThrottler, throttler);
        }

        req->Invoke().Subscribe(
            BIND([
                =,
                this,
                this_ = MakeStrong(this),
                throttlers = std::move(throttlers)
            ] (const TErrorOr<TDistributedThrottlerProxy::TRspHeartbeatPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_WARNING(rspOrError, "Failed updating throttler limit (LeaderId: %v)",
                        leaderId);
                    return;
                }

                const auto& rsp = rspOrError.Value();
                for (const auto& rspThrottler : rsp->throttlers()) {
                    auto limit = rspThrottler.has_limit() ? std::make_optional(rspThrottler.limit()) : std::nullopt;
                    const auto& throttlerId = rspThrottler.id();
                    const auto& throttler = GetOrCrash(throttlers, throttlerId);
                    throttler->SetLimit(limit);
                    YT_LOG_TRACE("Throttler limit updated (LeaderId: %v, ThrottlerId: %v, Limit: %v)",
                        leaderId,
                        throttlerId,
                        limit);
                }
            }));
    }

    void UpdateLeader()
    {
        if (!Active_.load()) {
            {
                auto guard = WriterGuard(Lock_);
                LeaderId_.reset();
                LeaderChannel_.Reset();
            }
            DistributedThrottlerService_->Finalize();
            return;
        }

        TListMembersOptions options;
        options.Limit = 1;
        options.AttributeKeys = {AddressAttributeKey, RealmIdAttributeKey};

        auto rspFuture = DiscoveryClient_->ListMembers(GroupId_, options);
        auto rspOrError = WaitForUnique(rspFuture);
        if (!rspOrError.IsOK()) {
            YT_LOG_WARNING(rspOrError, "Error updating leader");
            return;
        }

        const auto& members = rspOrError.Value();
        if (members.empty()) {
            return;
        }

        const auto& leader = members[0];
        auto optionalAddress = leader.Attributes->Find<TString>(AddressAttributeKey);
        if (!optionalAddress) {
            YT_LOG_WARNING("Leader does not have '%v' attribute (LeaderId: %v)",
                AddressAttributeKey,
                leader.Id);
            return;
        }

        auto optionalRealmId = leader.Attributes->Find<TRealmId>(RealmIdAttributeKey);
        if (!optionalRealmId) {
            YT_LOG_WARNING("Leader does not have '%v' attribute (LeaderId: %v)",
                RealmIdAttributeKey,
                leader.Id);
            return;
        }

        const auto& leaderId = members[0].Id;
        std::optional<TMemberId> oldLeaderId;
        IChannelPtr leaderChannel;
        {
            auto guard = WriterGuard(Lock_);
            if (LeaderId_ == leaderId) {
                return;
            }
            YT_LOG_INFO("Leader changed (OldLeaderId: %v, NewLeaderId: %v)",
                LeaderId_,
                leaderId);
            {
                auto* attributes = MemberClient_->GetAttributes();
                attributes->Set(LeaderIdAttributeKey, leaderId);
            }
            oldLeaderId = LeaderId_;
            LeaderId_ = leaderId;
            LeaderChannel_ = leaderId == MemberId_
                ? nullptr
                : CreateRealmChannel(ChannelFactory_->CreateChannel(*optionalAddress), *optionalRealmId);
            leaderChannel = LeaderChannel_;
        }

        if (Config_.Acquire()->Mode == EDistributedThrottlerMode::Precise) {
            auto guard = ReaderGuard(Throttlers_->Lock);
            for (const auto& [throttlerId, weakThrottler] : Throttlers_->Throttlers) {
                auto throttler = weakThrottler.Lock();
                if (!throttler) {
                    continue;
                }
                throttler->SetLeaderChannel(leaderChannel);
            }
        }

        if (oldLeaderId == MemberId_) {
            DistributedThrottlerService_->Finalize();
        }

        if (leaderId == MemberId_) {
            DistributedThrottlerService_->Initialize();
        }
    }

    void UpdateLocalThrottlersAttribute()
    {
        auto mode = Config_.Acquire()->Mode;

        YT_LOG_DEBUG("Update %Qv member attribute (Mode: %v)",
            LocalThrottlersAttributeKey,
            mode);

        auto guard = ReaderGuard(Throttlers_->Lock);
        auto yson = NYTree::BuildYsonStringFluently()
            .DoMapFor(Throttlers_->Throttlers, [&] (NYTree::TFluentMap fluent, const auto& item) {
                auto strongPtr = item.second.Lock();
                if (strongPtr) {
                    auto rate = std::round(strongPtr->GetUsageRate() * 10.0) / 10.0;
                    auto limit = std::round(strongPtr->GetLimit().value_or(0.0) * 10.0) / 10.0;
                    bool isPreciseMode = mode == EDistributedThrottlerMode::Precise;

                    fluent
                        .Item(item.first).BeginMap()
                            .Item("rate").Value(rate)
                            .Item("limit").Value(limit)
                            .DoIf(!isPreciseMode, [&] (auto fluent) {
                                // The following calls fail with verify in Precise mode.
                                fluent
                                    .Item("queue_total_amount").Value(strongPtr->GetQueueTotalAmount())
                                    .Item("is_overdraft").Value(strongPtr->IsOverdraft());
                            })
                            .Item("estimated_overdraft_duration").Value(strongPtr->GetEstimatedOverdraftDuration())
                            .Item("period").Value(strongPtr->GetConfig()->Period)
                        .EndMap();
                }
            });

        auto* attributes = MemberClient_->GetAttributes();
        attributes->SetYson(LocalThrottlersAttributeKey, yson);
    }

    void UpdateGlobalThrottlersAttribute()
    {
        auto throttlerToGlobalUsage = TryGetThrottlerToGlobalUsage();
        if (!throttlerToGlobalUsage || throttlerToGlobalUsage->empty()) {
            return;
        }

        YT_LOG_DEBUG("Update %Qv member attribute", GlobalThrottlersAttributeKey);

        auto yson = NYTree::BuildYsonStringFluently()
            .DoMapFor(*throttlerToGlobalUsage, [&] (NYTree::TFluentMap fluent, const auto& item) {
                auto rate = std::round(item.second.Rate * 10.0) / 10.0;
                auto limit = std::round(item.second.Limit * 10.0) / 10.0;
                fluent
                    .Item(item.first).BeginMap()
                        .Item("rate").Value(rate)
                        .Item("limit").Value(limit)
                        .Item("queue_total_amount").Value(item.second.QueueTotalAmount)
                        .Item("max_estimated_overdraft_duration").Value(item.second.MaxEstimatedOverdraftDuration)
                        .Item("min_estimated_overdraft_duration").Value(item.second.MinEstimatedOverdraftDuration)
                    .EndMap();
            });

        auto* attributes = MemberClient_->GetAttributes();
        attributes->SetYson(GlobalThrottlersAttributeKey, yson);
    }

    void UpdateThrottlersAttributes()
    {
        UpdateLocalThrottlersAttribute();
        UpdateGlobalThrottlersAttribute();
    }
};

////////////////////////////////////////////////////////////////////////////////

IDistributedThrottlerFactoryPtr CreateDistributedThrottlerFactory(
    TDistributedThrottlerConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IConnectionPtr connection,
    IInvokerPtr invoker,
    TGroupId groupId,
    TMemberId memberId,
    IServerPtr rpcServer,
    TString address,
    NLogging::TLogger logger,
    IAuthenticatorPtr authenticator,
    TProfiler profiler)
{
    return New<TDistributedThrottlerFactory>(
        CloneYsonStruct(std::move(config)),
        std::move(channelFactory),
        std::move(connection),
        std::move(invoker),
        std::move(groupId),
        std::move(memberId),
        std::move(rpcServer),
        std::move(address),
        std::move(logger),
        std::move(authenticator),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
