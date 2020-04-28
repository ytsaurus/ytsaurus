#include "distributed_throttler.h"
#include "distributed_throttler_proxy.h"
#include "config.h"

#include <yt/core/rpc/service_detail.h>

#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/periodic_executor.h>

#include <yt/ytlib/discovery_client/discovery_client.h>
#include <yt/ytlib/discovery_client/member_client.h>

#include <yt/core/misc/algorithm_helpers.h>
#include <yt/core/misc/historic_usage_aggregator.h>

namespace NYT::NDistributedThrottler {

using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const TString AddressAttributeKey = "address";
static const TString RealmIdAttributeKey = "realm_id";

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedThrottlerService)

class TDistributedThrottlerService
    : public TServiceBase
{
public:
    TDistributedThrottlerService(
        IServerPtr rpcServer,
        IInvokerPtr invoker,
        TDiscoveryClientPtr discoveryClient,
        TGroupId groupId,
        TDistributedThrottlerConfigPtr config,
        TRealmId realmId,
        NLogging::TLogger logger)
        : TServiceBase(
            invoker,
            TDistributedThrottlerProxy::GetDescriptor(),
            logger,
            realmId)
        , RpcServer_(std::move(rpcServer))
        , DiscoveryClient_(std::move(discoveryClient))
        , GroupId_(std::move(groupId))
        , Config_(std::move(config))
        , UpdatePeriodicExecutor_(New<TPeriodicExecutor>(
            std::move(invoker),
            BIND(&TDistributedThrottlerService::UpdateLimits, MakeWeak(this)),
            Config_->LimitUpdatePeriod))
        , Logger(std::move(logger))
        , MemberShards_(Config_->ShardCount)
        , ThrottlerShards_(Config_->ShardCount)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

    void Initialize()
    {
        RpcServer_->RegisterService(this);
        UpdatePeriodicExecutor_->Start();
    }

    void Finalize()
    {
        UpdatePeriodicExecutor_->Stop();
        RpcServer_->UnregisterService(this);
    }

    void SetTotalLimit(const TString& throttlerId, std::optional<i64> limit)
    {
        auto* shard = GetThrottlerShard(throttlerId);

        TWriterGuard guard(shard->TotalLimitsLock);
        shard->ThrottlerIdToTotalLimit[throttlerId] = limit;
    }

    void UpdateUsageRate(const TMemberId& memberId, THashMap<TString, i64> throttlerIdToUsageRate)
    {
        std::vector<std::vector<TString>> throttlerIdsByShard(Config_->ShardCount);
        for (const auto& [throttlerId, usageRate] : throttlerIdToUsageRate) {
            throttlerIdsByShard[GetShardIndex(throttlerId)].push_back(throttlerId);
        }

        auto now = TInstant::Now();
        for (int i = 0; i < Config_->ShardCount; ++i) {
            if (throttlerIdsByShard[i].empty()) {
                continue;
            }

            auto& shard = ThrottlerShards_[i];
            TWriterGuard guard(shard.LastUpdateTimeLock);
            for (const auto& throttlerId : throttlerIdsByShard[i]) {
                shard.ThrottlerIdToLastUpdateTime[throttlerId] = now;
            }
        }

        {
            auto* shard = GetMemberShard(memberId);

            TWriterGuard guard(shard->UsageRatesLock);
            shard->MemberIdToUsageRate[memberId].swap(throttlerIdToUsageRate);
        }
    }

    THashMap<TString, std::optional<i64>> GetMemberLimits(const TMemberId& memberId, const std::vector<TString>& throttlerIds)
    {
        std::vector<std::vector<TString>> throttlerIdsByShard(Config_->ShardCount);
        for (const auto& throttlerId : throttlerIds) {
            throttlerIdsByShard[GetShardIndex(throttlerId)].push_back(throttlerId);
        }

        THashMap<TString, std::optional<i64>> result;
        for (int i = 0; i < Config_->ShardCount; ++i) {
            if (throttlerIdsByShard[i].empty()) {
                continue;
            }

            auto& throttlerShard = ThrottlerShards_[i];
            TReaderGuard totalLimitsGuard(throttlerShard.TotalLimitsLock);
            for (const auto& throttlerId : throttlerIdsByShard[i]) {
                auto totalLimitIt = throttlerShard.ThrottlerIdToTotalLimit.find(throttlerId);
                if (totalLimitIt == throttlerShard.ThrottlerIdToTotalLimit.end()) {
                    YT_LOG_WARNING("There is no total limit for throttler (ThrottlerId: %v)", throttlerId);
                    continue;
                }

                auto optionalTotalLimit = totalLimitIt->second;
                if (!optionalTotalLimit) {
                    YT_VERIFY(result.emplace(throttlerId, std::nullopt).second);
                }
            }

            auto fillLimits = [&] (const THashMap<TString, i64>& throttlerIdToLimits) {
                for (const auto& throttlerId : throttlerIdsByShard[i]) {
                    if (result.contains(throttlerId)) {
                        continue;
                    }
                    auto limitIt = throttlerIdToLimits.find(throttlerId);
                    if (limitIt == throttlerIdToLimits.end()) {
                        YT_LOG_WARNING("There is no total limit for throttler (ThrottlerId: %v)", throttlerId);
                    } else {
                        YT_VERIFY(result.emplace(throttlerId, limitIt->second).second);
                    }
                }
            };

            if (Config_->DistributeLimitsUniformly) {
                TReaderGuard uniformLimitGuard(throttlerShard.UniformLimitLock);
                fillLimits(throttlerShard.ThrottlerIdToUniformLimit);
            } else {
                auto* shard = GetMemberShard(memberId);

                TReaderGuard limitsGuard(shard->LimitsLock);
                auto memberIt = shard->MemberIdToLimit.find(memberId);
                if (memberIt != shard->MemberIdToLimit.end()) {
                    fillLimits(memberIt->second);
                }
            }
        }

        return result;
    }

private:
    const IServerPtr RpcServer_;
    const TDiscoveryClientPtr DiscoveryClient_;
    const TString GroupId_;
    const TDistributedThrottlerConfigPtr Config_;
    const TPeriodicExecutorPtr UpdatePeriodicExecutor_;
    const NLogging::TLogger Logger;

    struct TMemberShard
    {
        TReaderWriterSpinLock LimitsLock;
        THashMap<TMemberId, THashMap<TString, i64>> MemberIdToLimit;

        TReaderWriterSpinLock UsageRatesLock;
        THashMap<TMemberId, THashMap<TString, i64>> MemberIdToUsageRate;
    };
    std::vector<TMemberShard> MemberShards_;

    struct TThrottlerShard
    {
        TReaderWriterSpinLock TotalLimitsLock;
        THashMap<TString, std::optional<i64>> ThrottlerIdToTotalLimit;

        TReaderWriterSpinLock UniformLimitLock;
        THashMap<TString, i64> ThrottlerIdToUniformLimit;

        TReaderWriterSpinLock LastUpdateTimeLock;
        THashMap<TString, TInstant> ThrottlerIdToLastUpdateTime;
    };
    std::vector<TThrottlerShard> ThrottlerShards_;

    DECLARE_RPC_SERVICE_METHOD(NDistributedThrottler::NProto, Heartbeat)
    {
        const auto& memberId = request->member_id();

        context->SetRequestInfo("MemberId: %v, ThrottlerCount: %v",
            memberId,
            request->throttlers().size());

        THashMap<TString, i64> throttlerIdToUsageRate;
        for (const auto& throttler : request->throttlers()) {
            const auto& throttlerId = throttler.id();
            auto usageRate = throttler.usage_rate();
            YT_VERIFY(throttlerIdToUsageRate.emplace(throttlerId, usageRate).second);
        }

        auto limits = GetMemberLimits(memberId, GetKeys(throttlerIdToUsageRate));
        for (const auto& [throttlerId, limit] : limits) {
            auto* result = response->add_throttlers();
            result->set_id(throttlerId);
            if (limit) {
                result->set_limit(*limit);
            }
        }
        UpdateUsageRate(memberId, std::move(throttlerIdToUsageRate));

        context->Reply();
    }

    int GetShardIndex(const TMemberId& memberId)
    {
        return THash<TMemberId>()(memberId) % Config_->ShardCount;
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
        auto countRspOrError = WaitFor(DiscoveryClient_->GetGroupMeta(GroupId_));
        if (!countRspOrError.IsOK()) {
            YT_LOG_WARNING(countRspOrError, "Error updating throttler limits");
            return;
        }

        auto totalCount = countRspOrError.Value().MemberCount;
        if (totalCount == 0) {
            YT_LOG_WARNING("No members in current group");
            return;
        }

        for (auto& shard : ThrottlerShards_) {
            THashMap<TString, i64> throttlerIdToUniformLimit;
            {
                TReaderGuard guard(shard.TotalLimitsLock);
                for (const auto& [throttlerId, optionalTotalLimit] : shard.ThrottlerIdToTotalLimit) {
                    if (!optionalTotalLimit) {
                        continue;
                    }

                    auto uniformLimit = std::max<i64>(1, *optionalTotalLimit / totalCount);
                    YT_VERIFY(throttlerIdToUniformLimit.emplace(throttlerId, uniformLimit).second);
                    YT_LOG_TRACE("Uniform distribution limit updated (ThrottlerId: %v, UniformLimit: %v)",
                        throttlerId,
                        uniformLimit);
                }
            }

            {
                TWriterGuard guard(shard.UniformLimitLock);
                shard.ThrottlerIdToUniformLimit.swap(throttlerIdToUniformLimit);
            }
        }
    }

    void UpdateLimits()
    {
        ForgetDeadThrottlers();

        if (Config_->DistributeLimitsUniformly) {
            UpdateUniformLimitDistribution();
            return;
        }

        std::vector<THashMap<TMemberId, THashMap<TString, i64>>> memberIdToLimit(Config_->ShardCount);
        for (auto& throttlerShard : ThrottlerShards_) {
            THashMap<TString, std::optional<i64>> throttlerIdToTotalLimit;
            {
                TReaderGuard guard(throttlerShard.TotalLimitsLock);
                throttlerIdToTotalLimit = throttlerShard.ThrottlerIdToTotalLimit;
            }

            THashMap<TString, i64> throttlerIdToTotalUsage;
            THashMap<TString, THashMap<TString, i64>> throttlerIdToUsageRates;
            int memberCount = 0;

            for (const auto& shard : MemberShards_) {
                TReaderGuard guard(shard.UsageRatesLock);
                memberCount += shard.MemberIdToUsageRate.size();
                for (const auto& [memberId, throttlers] : shard.MemberIdToUsageRate) {
                    for (const auto& [throttlerId, totalLimit] : throttlerIdToTotalLimit) {
                        auto throttlerIt = throttlers.find(throttlerId);
                        if (throttlerIt == throttlers.end()) {
                            YT_LOG_INFO("Member doesn't know about throttler (MemberId: %v, ThrottlerId: %v)",
                                memberId,
                                throttlerId);
                            continue;
                        }
                        auto usageRate = throttlerIt->second;
                        throttlerIdToTotalUsage[throttlerId] += usageRate;
                        throttlerIdToUsageRates[throttlerId].emplace(memberId, usageRate);
                    }
                }
            }

            for (const auto& [throttlerId, totalUsageRate] : throttlerIdToTotalUsage) {
                auto optionalTotalLimit = GetOrCrash(throttlerIdToTotalLimit, throttlerId);
                if (!optionalTotalLimit) {
                    continue;
                }
                auto totalLimit = *optionalTotalLimit;

                auto defaultLimit = BinarySearch<i64>(0, totalLimit, [&, &throttlerId = throttlerId](i64 value) {
                    i64 total = 0;
                    for (const auto& [memberId, usageRate] : throttlerIdToUsageRates[throttlerId]) {
                        total += Min(value, usageRate);
                    }
                    return total < totalLimit;
                });

                auto extraLimit = (Config_->ExtraLimitRatio * totalLimit + Max<i64>(0, totalLimit - totalUsageRate)) / memberCount + 1;

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
                }
            }
        }

        {
            for (int i = 0; i < Config_->ShardCount; ++i) {
                auto& shard = MemberShards_[i];
                TReaderGuard guard(shard.LimitsLock);
                shard.MemberIdToLimit.swap(memberIdToLimit[i]);
            }
        }
    }

    void ForgetDeadThrottlers()
    {
        for (auto& throttlerShard : ThrottlerShards_) {
            std::vector<TString> deadThrottlersIds;

            {
                auto now = TInstant::Now();
                TReaderGuard guard(throttlerShard.LastUpdateTimeLock);
                for (const auto& [throttlerId, lastUpdateTime] : throttlerShard.ThrottlerIdToLastUpdateTime) {
                    if (lastUpdateTime + Config_->ThrottlerExpirationTime < now) {
                        deadThrottlersIds.push_back(throttlerId);
                    }
                }
            }

            if (deadThrottlersIds.empty()) {
                continue;
            }

            {

                TWriterGuard guard(throttlerShard.TotalLimitsLock);
                for (const auto& deadThrottlerId : deadThrottlersIds) {
                    throttlerShard.ThrottlerIdToTotalLimit.erase(deadThrottlerId);
                }
            }

            {
                TWriterGuard guard(throttlerShard.UniformLimitLock);
                for (const auto& deadThrottlerId : deadThrottlersIds) {
                    throttlerShard.ThrottlerIdToUniformLimit.erase(deadThrottlerId);
                }
            }

            for (auto& memberShard : MemberShards_) {
                TWriterGuard guard(memberShard.LimitsLock);
                for (auto& [memberId, throttlerIdToLimit] : memberShard.MemberIdToLimit) {
                    for (const auto& deadThrottlerId : deadThrottlersIds) {
                        throttlerIdToLimit.erase(deadThrottlerId);
                    }
                }
            }

            for (auto& memberShard : MemberShards_) {
                TWriterGuard guard(memberShard.UsageRatesLock);
                for (auto& [memberId, throttlerIdToUsageRate] : memberShard.MemberIdToUsageRate) {
                    for (const auto& deadThrottlerId : deadThrottlersIds) {
                        throttlerIdToUsageRate.erase(deadThrottlerId);
                    }
                }
            }

            {
                TWriterGuard guard(throttlerShard.LastUpdateTimeLock);
                for (const auto& deadThrottlerId : deadThrottlersIds) {
                    throttlerShard.ThrottlerIdToLastUpdateTime.erase(deadThrottlerId);
                }
            }
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerService)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TWrappedThrottler)

class TWrappedThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    TWrappedThrottler(
        TDistributedThrottlerConfigPtr config,
        TThroughputThrottlerConfigPtr throttlerConfig)
        : Underlying_(CreateReconfigurableThroughputThrottler(throttlerConfig))
        , Config_(std::move(throttlerConfig))
    {
        HistoricUsageAggregator_.UpdateParameters(THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::ExponentialMovingAverage,
            config->EmaAlpha
        ));
    }

    double GetUsageRate()
    {
        return HistoricUsageAggregator_.GetHistoricUsage();
    }

    const TThroughputThrottlerConfigPtr& GetConfig()
    {
        return Config_;
    }

    TFuture<void> Throttle(i64 count)
    {
        auto future = Underlying_->Throttle(count);
        future.Subscribe(BIND([=] (const TError& error) {
            if (error.IsOK()) {
                HistoricUsageAggregator_.UpdateAt(TInstant::Now(), count);
            }
        }));
        return future;
    }

    bool TryAcquire(i64 count)
    {
        auto result = Underlying_->TryAcquire(count);
        if (result) {
            HistoricUsageAggregator_.UpdateAt(TInstant::Now(), count);
        }
        return result;
    }

    i64 TryAcquireAvailable(i64 count)
    {
        auto result = Underlying_->TryAcquireAvailable(count);
        if (result > 0) {
            HistoricUsageAggregator_.UpdateAt(TInstant::Now(), result);
        }
        return result;
    }

    void Acquire(i64 count)
    {
        HistoricUsageAggregator_.UpdateAt(TInstant::Now(), count);
        Underlying_->Acquire(count);
    }

    bool IsOverdraft()
    {
        return Underlying_->IsOverdraft();
    }

    i64 GetQueueTotalCount() const
    {
        return Underlying_->GetQueueTotalCount();
    }

    void Reconfigure(TThroughputThrottlerConfigPtr config)
    {
        Config_ = CloneYsonSerializable(std::move(config));
    }

    void DoReconfigure(TThroughputThrottlerConfigPtr config)
    {
        Underlying_->Reconfigure(std::move(config));
    }

private:
    const IReconfigurableThroughputThrottlerPtr Underlying_;

    TThroughputThrottlerConfigPtr Config_;

    THistoricUsageAggregator HistoricUsageAggregator_;
};

DEFINE_REFCOUNTED_TYPE(TWrappedThrottler)

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerFactory::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TDistributedThrottlerConfigPtr config,
        IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        TGroupId groupId,
        TMemberId memberId,
        IServerPtr rpcServer,
        TString address,
        NLogging::TLogger logger)
        : Config_(std::move(config))
        , ChannelFactory_(std::move(channelFactory))
        , GroupId_(std::move(groupId))
        , MemberId_(std::move(memberId))
        , MemberClient_(New<TMemberClient>(
            Config_->MemberClient,
            ChannelFactory_,
            invoker,
            MemberId_,
            GroupId_))
        , DiscoveryClient_(New<TDiscoveryClient>(
            Config_->DiscoveryClient,
            ChannelFactory_))
        , UpdateLimitsExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TImpl::UpdateLimits, MakeWeak(this)),
            Config_->LimitUpdatePeriod))
        , UpdateLeaderExecutor_(New<TPeriodicExecutor>(
            invoker,
            BIND(&TImpl::UpdateLeader, MakeWeak(this)),
            Config_->LeaderUpdatePeriod))
        , RealmId_(TGuid::Create())
        , Logger(NLogging::TLogger(logger)
            .AddTag("SelfMemberId: %v", MemberId_)
            .AddTag("GroupId: %v", GroupId_)
            .AddTag("RealmId: %v", RealmId_))
        , DistributedThrottlerService_(New<TDistributedThrottlerService>(
            std::move(rpcServer),
            std::move(invoker),
            DiscoveryClient_,
            GroupId_,
            Config_,
            RealmId_,
            Logger))
    {
        auto* attributes = MemberClient_->GetAttributes();
        attributes->Set(RealmIdAttributeKey, RealmId_);
        attributes->Set(AddressAttributeKey, address);

        MemberClient_->SetPriority(TInstant::Now().Seconds());
        MemberClient_->Start();

        UpdateLimitsExecutor_->Start();
        UpdateLeaderExecutor_->Start();
    }

    ~TImpl()
    {
        MemberClient_->Stop();
        UpdateLimitsExecutor_->Stop();
        UpdateLeaderExecutor_->Stop();

        if (LeaderId_ == MemberId_) {
            DistributedThrottlerService_->Finalize();
        }
    }

    IReconfigurableThroughputThrottlerPtr GetOrCreateThrottler(const TString& throttlerId, TThroughputThrottlerConfigPtr throttlerConfig)
    {
        auto findThrottler = [&] (const TString& throttlerId) -> IReconfigurableThroughputThrottlerPtr {
            auto it = Throttlers_.find(throttlerId);
            if (it == Throttlers_.end()) {
                return nullptr;
            }
            auto throttler = it->second.Lock();
            if (!throttler) {
                return nullptr;
            }
            throttler->Reconfigure(std::move(throttlerConfig));
            return throttler;
        };

        {
            TReaderGuard guard(Lock_);
            auto throttler = findThrottler(throttlerId);
            if (throttler) {
                return throttler;
            }
        }

        {
            TWriterGuard guard(Lock_);
            auto throttler = findThrottler(throttlerId);
            if (throttler) {
                return throttler;
            }

            DistributedThrottlerService_->SetTotalLimit(throttlerId, throttlerConfig->Limit);
            auto wrappedThrottler = New<TWrappedThrottler>(Config_, std::move(throttlerConfig));
            Throttlers_[throttlerId] = wrappedThrottler;

            return wrappedThrottler;
        }
    }

private:
    const TDistributedThrottlerConfigPtr Config_;
    const IChannelFactoryPtr ChannelFactory_;
    const TGroupId GroupId_;
    const TMemberId MemberId_;
    const TMemberClientPtr MemberClient_;
    const TDiscoveryClientPtr DiscoveryClient_;
    const TPeriodicExecutorPtr UpdateLimitsExecutor_;
    const TPeriodicExecutorPtr UpdateLeaderExecutor_;
    const TRealmId RealmId_;
    const NLogging::TLogger Logger;

    TReaderWriterSpinLock ThrottlersLock_;
    THashMap<TString, TWeakPtr<TWrappedThrottler>> Throttlers_;

    TReaderWriterSpinLock Lock_;
    std::optional<TMemberId> LeaderId_;
    IChannelPtr LeaderChannel_;

    TDistributedThrottlerServicePtr DistributedThrottlerService_;

    void UpdateThroughputThrottlerLimit(const TWrappedThrottlerPtr& throttler, std::optional<i64> limit)
    {
        auto config = CloneYsonSerializable(throttler->GetConfig());
        config->Limit = limit;
        throttler->DoReconfigure(config);
    }

    void UpdateLimits()
    {
        std::optional<TMemberId> optionalCurrentLeaderId;
        {
            TReaderGuard guard(Lock_);
            optionalCurrentLeaderId = LeaderId_;
        }

        if (!optionalCurrentLeaderId) {
            UpdateLeader();
        }

        THashMap<TString, TWrappedThrottlerPtr> throttlers;
        std::vector<TString> deadThrottlers;
        {
            TReaderGuard guard(ThrottlersLock_);

            for (const auto& [throttlerId, throttler] : Throttlers_) {
                if (auto throttlerPtr = throttler.Lock()) {
                    YT_VERIFY(throttlers.emplace(throttlerId, throttlerPtr).second);
                } else {
                    deadThrottlers.push_back(throttlerId);
                }
            }
        }

        if (!deadThrottlers.empty()) {
            TWriterGuard guard(ThrottlersLock_);
            for (const auto& throttlerId : deadThrottlers) {
                Throttlers_.erase(throttlerId);
            }
        }

        if (optionalCurrentLeaderId == MemberId_) {
            THashMap<TString, i64> throttlerIdToUsageRate;
            for (const auto& [throttlerId, throttler] : throttlers) {
                const auto& config = throttler->GetConfig();
                DistributedThrottlerService_->SetTotalLimit(throttlerId, config->Limit);

                auto usageRate = throttler->GetUsageRate();
                YT_VERIFY(throttlerIdToUsageRate.emplace(throttlerId, usageRate).second);
            }

            auto limits = DistributedThrottlerService_->GetMemberLimits(MemberId_, GetKeys(throttlerIdToUsageRate));
            for (const auto& [throttlerId, limit] : limits) {
                const auto& throttler = GetOrCrash(throttlers, throttlerId);
                UpdateThroughputThrottlerLimit(throttler, limit);
                YT_LOG_TRACE("Throttler limit updated (ThrottlerId: %v, Limit: %v)",
                    throttlerId,
                    limit);
            }
            DistributedThrottlerService_->UpdateUsageRate(MemberId_, std::move(throttlerIdToUsageRate));
            return;
        }

        IChannelPtr currentLeaderChannel;
        TMemberId currentLeaderId;
        {
            TReaderGuard guard(Lock_);
            if (!LeaderId_ || !LeaderChannel_) {
                YT_LOG_WARNING("Failed updating throttler limit: no active leader");
                return;
            }
            currentLeaderId = *LeaderId_;
            currentLeaderChannel = LeaderChannel_;
        }

        TDistributedThrottlerProxy proxy(currentLeaderChannel);

        auto req = proxy.Heartbeat();
        req->SetTimeout(Config_->RpcTimeout);
        req->set_member_id(MemberId_);

        for (const auto& [throttlerId, throttler] : throttlers) {
            auto* protoThrottler = req->add_throttlers();
            protoThrottler->set_id(throttlerId);
            protoThrottler->set_usage_rate(throttler->GetUsageRate());
        }

        req->Invoke().Subscribe(
            BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TDistributedThrottlerProxy::TRspHeartbeatPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_WARNING(rspOrError, "Failed updating throttler limit (LeaderId: %v)", currentLeaderId);
                    return;
                }

                const auto& rsp = rspOrError.Value();
                for (const auto& rspThrottler : rsp->throttlers()) {
                    auto limit = rspThrottler.has_limit() ? std::make_optional(rspThrottler.limit()) : std::nullopt;
                    const auto& throttlerId = rspThrottler.id();
                    const auto& throttler = GetOrCrash(throttlers, throttlerId);
                    UpdateThroughputThrottlerLimit(throttler, limit);
                    YT_LOG_TRACE("Throttler limit updated (LeaderId: %v, ThrottlerId: %v, Limit: %v)",
                        currentLeaderId,
                        throttlerId,
                        limit);
                }
            }));
    }

    void UpdateLeader()
    {
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
        {
            TWriterGuard guard(Lock_);
            if (LeaderId_ == leaderId) {
                return;
            }
            YT_LOG_INFO("Leader changed (OldLeaderId: %v, NewLeaderId: %v)",
                LeaderId_,
                leaderId);
            oldLeaderId = LeaderId_;
            LeaderId_ = leaderId;
            LeaderChannel_ = CreateRealmChannel(ChannelFactory_->CreateChannel(*optionalAddress), *optionalRealmId);
        }

        if (oldLeaderId == MemberId_) {
            DistributedThrottlerService_->Finalize();
        }

        if (leaderId == MemberId_) {
            DistributedThrottlerService_->Initialize();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TDistributedThrottlerFactory::TDistributedThrottlerFactory(
    TDistributedThrottlerConfigPtr config,
    IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    TGroupId groupId,
    TMemberId memberId,
    IServerPtr rpcServer,
    TString address,
    NLogging::TLogger logger)
    : Impl_(New<TImpl>(
        std::move(config),
        std::move(channelFactory),
        std::move(invoker),
        std::move(groupId),
        std::move(memberId),
        std::move(rpcServer),
        std::move(address),
        std::move(logger)))
{ }

TDistributedThrottlerFactory::~TDistributedThrottlerFactory() = default;

IReconfigurableThroughputThrottlerPtr TDistributedThrottlerFactory::GetOrCreateThrottler(
    const TString& throttlerId,
    TThroughputThrottlerConfigPtr throttlerConfig)
{
    return Impl_->GetOrCreateThrottler(
        throttlerId,
        std::move(throttlerConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
