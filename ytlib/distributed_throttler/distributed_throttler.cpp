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
        i64 limit,
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
        , TotalLimit_(limit)
        , UniformLimit_(limit)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Heartbeat));
    }

    void Initialize()
    {
        RpcServer_->RegisterService(this);

        UpdatePeriodicExecutor_->Start();
        UpdateUniformLimitDistribution();
    }

    void Finalize()
    {
        UpdatePeriodicExecutor_->Stop();

        RpcServer_->UnregisterService(this);
    }

    void SetTotalLimit(i64 limit)
    {
        TotalLimit_ = limit;
    }

    std::optional<i64> GetMemberLimit(const TString& memberId, i64 usageRate)
    {
        if (TotalLimit_ < 0) {
            return std::nullopt;
        }

        if (Config_->DistributeLimitsUniformly) {
            return UniformLimit_;
        }

        UpdateUsageRate(memberId, usageRate);

        TReaderGuard guard(LimitsLock_);
        auto it = MemberToLimit_.find(memberId);
        return it == MemberToLimit_.end() ? UniformLimit_ : it->second;
    }

private:
    const IServerPtr RpcServer_;
    const TDiscoveryClientPtr DiscoveryClient_;
    const TString GroupId_;
    const TDistributedThrottlerConfigPtr Config_;
    const TPeriodicExecutorPtr UpdatePeriodicExecutor_;
    const NLogging::TLogger Logger;


    std::atomic<i64> TotalLimit_;
    i64 UniformLimit_;

    TReaderWriterSpinLock LimitsLock_;
    THashMap<TMemberId, i64> MemberToLimit_;

    TSpinLock UsageRateLock_;
    THashMap<TMemberId, i64> MemberToUsageRate_;

    DECLARE_RPC_SERVICE_METHOD(NDistributedThrottler::NProto, Heartbeat)
    {
        const auto& memberId = request->member_id();
        auto usageRate = request->usage_rate();

        context->SetRequestInfo("MemberId: %v, UsageRate: %v",
            memberId,
            usageRate);

        auto limit = GetMemberLimit(memberId, usageRate);
        if (limit) {
            response->set_limit(*limit);
            context->SetResponseInfo("Limit: %v", limit);
        }
        context->Reply();
    }

    void UpdateUniformLimitDistribution()
    {
        auto countRspOrError = WaitFor(DiscoveryClient_->GetGroupSize(GroupId_));
        if (!countRspOrError.IsOK()) {
            YT_LOG_WARNING(countRspOrError, "Error updating throttler limits");
            return;
        }

        auto totalCount = countRspOrError.Value();
        if (totalCount == 0) {
            YT_LOG_WARNING("No members in current group");
            return;
        }

        UniformLimit_ = TotalLimit_.load() / totalCount;
        YT_LOG_DEBUG("Uniform distribution limit updated (Value: %v)", UniformLimit_);
    }

    void UpdateUsageRate(const TMemberId& memberId, i64 usageRate)
    {
        TGuard guard(UsageRateLock_);
        MemberToUsageRate_[memberId] = usageRate;
    }

    void UpdateLimits()
    {
        i64 totalLimit = TotalLimit_.load();
        if (totalLimit < 0) {
            return;
        }

        UpdateUniformLimitDistribution();

        if (Config_->DistributeLimitsUniformly) {
            return;
        }

        i64 totalLimits = 0;
        THashMap<TMemberId, i64> memberToUsageRate;
        {
            TGuard guard(UsageRateLock_);
            memberToUsageRate = MemberToUsageRate_;
        }

        for (const auto& [memberId, usageRate] : memberToUsageRate) {
            totalLimits += usageRate;
        }

        auto defaultLimit = BinarySearch<i64>(0, totalLimit, [&] (i64 value) {
            i64 total = 0;
            for (const auto& [memberId, usageRate] : memberToUsageRate) {
                total += Min(value, usageRate);
            }
            return total < totalLimit;
        });

        auto extraLimit = (Config_->ExtraLimitRatio * totalLimit + Max<i64>(0, totalLimit - totalLimits)) / memberToUsageRate.size() + 1;
        THashMap<TMemberId, i64> memberToLimit;
        memberToLimit.reserve(memberToUsageRate.size());
        for (const auto& [memberId, usageRate] : memberToUsageRate) {
            auto newLimit = Min(usageRate, defaultLimit) + extraLimit;
            YT_LOG_INFO("Updating throttler limit (MemberId: %v, UsageRate: %v, NewLimit: %v, ExtraLimit: %v)",
                memberId,
                usageRate,
                newLimit,
                extraLimit);
            YT_VERIFY(memberToLimit.emplace(memberId, newLimit).second);
        }

        {
            TWriterGuard guard(LimitsLock_);
            MemberToLimit_.swap(memberToLimit);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TDistributedThrottlerService)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedThrottler)

class TDistributedThrottler
    : public IReconfigurableThroughputThrottler
{
public:
    TDistributedThrottler(
        TDistributedThrottlerConfigPtr config,
        TThroughputThrottlerConfigPtr throttlerConfig,
        IChannelFactoryPtr channelFactory,
        IInvokerPtr invoker,
        TGroupId groupId,
        TMemberId memberId,
        IServerPtr rpcServer,
        TString address,
        NLogging::TLogger logger)
    : Config_(std::move(config))
    , ThrottlerConfig_(CloneYsonSerializable(std::move(throttlerConfig)))
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
    , Throttler_(CreateReconfigurableThroughputThrottler(
        ThrottlerConfig_))
    , UpdateLimitsExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDistributedThrottler::UpdateLimits, MakeWeak(this)),
        Config_->LimitUpdatePeriod))
    , UpdateLeaderExecutor_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TDistributedThrottler::UpdateLeader, MakeWeak(this)),
        Config_->LeaderUpdatePeriod))
    , Logger(NLogging::TLogger(logger)
        .AddTag("SelfMemberId: %v", MemberId_)
        .AddTag("GroupId: %v", GroupId_))
    , RealmId_(TGuid::Create())
    , DistributedThrottlerService_(New<TDistributedThrottlerService>(
        std::move(rpcServer),
        std::move(invoker),
        DiscoveryClient_,
        GroupId_,
        ThrottlerConfig_->Limit.value_or(0),
        Config_,
        RealmId_,
        Logger))
    {
        HistoricUsageAggregator_.UpdateParameters(THistoricUsageAggregationParameters(
            EHistoricUsageAggregationMode::ExponentialMovingAverage,
            Config_->EmaAlpha
        ));

        auto* attributes = MemberClient_->GetAttributes();
        attributes->Set(RealmIdAttributeKey, RealmId_);
        attributes->Set(AddressAttributeKey, address);

        MemberClient_->SetPriority(TInstant::Now().Seconds());
        MemberClient_->Start();

        UpdateLimitsExecutor_->Start();
        UpdateLeaderExecutor_->Start();
    }

    ~TDistributedThrottler()
    {
        MemberClient_->Stop();
        UpdateLimitsExecutor_->Stop();
        UpdateLeaderExecutor_->Stop();

        if (LeaderId_ == MemberId_) {
            DistributedThrottlerService_->Finalize();
        }
    }

    TFuture<void> Throttle(i64 count)
    {
        auto future = Throttler_->Throttle(count);
        future.Subscribe(BIND([=] (const TError& error) {
            if (error.IsOK()) {
                HistoricUsageAggregator_.UpdateAt(TInstant::Now(), count);
            }
        }));
        return future;
    }

    bool TryAcquire(i64 count)
    {
        auto result = Throttler_->TryAcquire(count);
        if (result) {
            HistoricUsageAggregator_.UpdateAt(TInstant::Now(), count);
        }
        return result;
    }

    i64 TryAcquireAvailable(i64 count)
    {
        auto result = Throttler_->TryAcquireAvailable(count);
        if (result > 0) {
            HistoricUsageAggregator_.UpdateAt(TInstant::Now(), result);
        }
        return result;
    }

    void Acquire(i64 count)
    {
        HistoricUsageAggregator_.UpdateAt(TInstant::Now(), count);
        Throttler_->Acquire(count);
    }

    bool IsOverdraft()
    {
        return Throttler_->IsOverdraft();
    }

    i64 GetQueueTotalCount() const
    {
        return Throttler_->GetQueueTotalCount();
    }

    void Reconfigure(TThroughputThrottlerConfigPtr config)
    {
        DistributedThrottlerService_->SetTotalLimit(config->Limit.value_or(-1));
        // Limit will be updated in UpdateLimits.
        ThrottlerConfig_->Period = config->Period;
    }

private:
    const TDistributedThrottlerConfigPtr Config_;
    const TThroughputThrottlerConfigPtr ThrottlerConfig_;
    const IChannelFactoryPtr ChannelFactory_;
    const TGroupId GroupId_;
    const TMemberId MemberId_;
    const TMemberClientPtr MemberClient_;
    const TDiscoveryClientPtr DiscoveryClient_;
    const IReconfigurableThroughputThrottlerPtr Throttler_;
    const TPeriodicExecutorPtr UpdateLimitsExecutor_;
    const TPeriodicExecutorPtr UpdateLeaderExecutor_;
    const NLogging::TLogger Logger;
    const TRealmId RealmId_;

    TReaderWriterSpinLock Lock_;
    std::optional<TMemberId> LeaderId_;
    IChannelPtr LeaderChannel_;

    THistoricUsageAggregator HistoricUsageAggregator_;

    TIntrusivePtr<TDistributedThrottlerService> DistributedThrottlerService_;

    void UpdateThroughputThrottlerLimit(std::optional<i64> limit)
    {
        ThrottlerConfig_->Limit = limit;
        Throttler_->Reconfigure(ThrottlerConfig_);
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

        auto usageRate = HistoricUsageAggregator_.GetHistoricUsage();

        if (optionalCurrentLeaderId == MemberId_) {
            auto limit = DistributedThrottlerService_->GetMemberLimit(MemberId_, usageRate);
            UpdateThroughputThrottlerLimit(limit);
            YT_LOG_DEBUG("Throttler limit updated (Limit: %v)", limit);
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
        req->set_usage_rate(usageRate);
        req->Invoke().Subscribe(
            BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TDistributedThrottlerProxy::TRspHeartbeatPtr>& rspOrError) {
                if (!rspOrError.IsOK()) {
                    YT_LOG_WARNING(rspOrError, "Failed updating throttler limit (LeaderId: %v)", currentLeaderId);
                    return;
                }

                std::optional<i64> limit = std::nullopt;
                const auto& value = rspOrError.Value();
                if (value->has_limit()) {
                    limit = value->limit();
                }

                UpdateThroughputThrottlerLimit(limit);
                YT_LOG_DEBUG("Throttler limit updated (LeaderId: %v, Limit: %v)",
                    currentLeaderId,
                    limit);
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

DEFINE_REFCOUNTED_TYPE(TDistributedThrottler)

////////////////////////////////////////////////////////////////////////////////

IReconfigurableThroughputThrottlerPtr CreateDistributedThrottler(
    TDistributedThrottlerConfigPtr config,
    NConcurrency::TThroughputThrottlerConfigPtr throttlerConfig,
    NRpc::IChannelFactoryPtr channelFactory,
    IInvokerPtr invoker,
    NDiscoveryClient::TGroupId groupId,
    NDiscoveryClient::TMemberId memberId,
    NRpc::IServerPtr rpcServer,
    TString address,
    NLogging::TLogger logger)
{
    return New<TDistributedThrottler>(
        std::move(config),
        std::move(throttlerConfig),
        std::move(channelFactory),
        std::move(invoker),
        std::move(groupId),
        std::move(memberId),
        std::move(rpcServer),
        std::move(address),
        std::move(logger));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedThrottler
