#include "public.h"
#include "throttler_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/server/lib/exec_node/config.h>

#include <yt/yt/server/lib/misc/cluster_throttlers_config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/distributed_throttler/config.h>
#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

using namespace NYTree;
using namespace NDataNode;
using namespace NScheduler;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDiscoveryClient;
using namespace NDistributedThrottler;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

class TThrottlerManager
    : public IThrottlerManager
{
public:
    TThrottlerManager(
        NClusterNode::IBootstrap* bootstrap,
        TThrottlerManagerOptions options);

    IThroughputThrottlerPtr GetOrCreateThrottler(
        EExecNodeThrottlerKind kind,
        EThrottlerTrafficType trafficType,
        std::optional<TClusterName> remoteClusterName) override;

    void Reconfigure(TClusterNodeDynamicConfigPtr dynamicConfig) override;

    const TClusterThrottlersConfigPtr GetClusterThrottlersConfig() const override;

    std::optional<THashMap<TClusterName, TIncomingTrafficUtilization>> GetClusterToIncomingTrafficUtilization(
        EThrottlerTrafficType trafficType) const override;

private:
    NClusterNode::IBootstrap* const Bootstrap_ = nullptr;
    // Fields from bootstrap.
    const NRpc::IAuthenticatorPtr Authenticator_;
    const NApi::NNative::IConnectionPtr Connection_;
    const NApi::NNative::IClientPtr Client_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TClusterNodeBootstrapConfigPtr ClusterNodeConfig_;
    const IInvokerPtr Invoker_;
    const NRpc::IServerPtr RpcServer_;
    // Fields from manager options.
    const std::string LocalAddress_;
    const NProfiling::TProfiler Profiler_;
    NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr ClusterThrottlersConfigUpdater_;

    // The following members are protected by Lock_.
    TClusterThrottlersConfigPtr ClusterThrottlersConfig_;
    IDistributedThrottlerFactoryPtr DistributedThrottlerFactory_;
    struct TThroughputThrottlerData
    {
        IThroughputThrottlerPtr Throttler;
        TThroughputThrottlerConfigPtr ThrottlerConfig;
    };
    std::map<TThrottlerId, TThroughputThrottlerData> DistributedThrottlersHolder_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    static EDataNodeThrottlerKind GetDataNodeThrottlerKind(EExecNodeThrottlerKind kind);

    static bool NeedDistributedThrottlerFactory(TClusterThrottlersConfigPtr config);

    static TThrottlerId ToThrottlerId(EThrottlerTrafficType trafficType, const TClusterName& clusterName);
    static std::pair<EThrottlerTrafficType, TClusterName> FromThrottlerId(const TThrottlerId& throttlerId);

    void TryUpdateClusterThrottlersConfig();
    //! Lock_ has to be taken prior to calling this method.
    void UpdateDistributedThrottlers();
    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr GetLocalThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType trafficType) const;
    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr GetOrCreateDistributedThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType trafficType, std::optional<TClusterName> remoteClusterName);
};

////////////////////////////////////////////////////////////////////////////////

EDataNodeThrottlerKind TThrottlerManager::GetDataNodeThrottlerKind(EExecNodeThrottlerKind kind)
{
    switch (kind) {
        case EExecNodeThrottlerKind::ArtifactCacheIn:
            return EDataNodeThrottlerKind::ArtifactCacheIn;
        case EExecNodeThrottlerKind::JobIn:
            return EDataNodeThrottlerKind::JobIn;
        case EExecNodeThrottlerKind::JobOut:
            return EDataNodeThrottlerKind::JobOut;
        default:
            YT_ABORT();
    }
}

bool TThrottlerManager::NeedDistributedThrottlerFactory(TClusterThrottlersConfigPtr config)
{
    if (!config || !config->Enabled) {
        return false;
    }

    return !config->ClusterLimits.empty();
}

TThrottlerId TThrottlerManager::ToThrottlerId(EThrottlerTrafficType trafficType, const TClusterName& clusterName)
{
    return Format("%lv_%v", trafficType, clusterName.Underlying());
}

std::pair<EThrottlerTrafficType, TClusterName> TThrottlerManager::FromThrottlerId(const TThrottlerId& throttlerId)
{
    for (auto trafficType : TEnumTraits<EThrottlerTrafficType>::GetDomainValues()) {
        auto trafficTypeString = Format("%lv_", trafficType);
        if (throttlerId.StartsWith(trafficTypeString)) {
            return {trafficType, TClusterName(throttlerId.substr(trafficTypeString.size()))};
        }
    }
    THROW_ERROR_EXCEPTION("Invalid throttler id %Qv", throttlerId);
}

void TThrottlerManager::UpdateDistributedThrottlers()
{
    // Recreate throttlers with updated config.
    for (const auto& [clusterName, clusterLimits] : ClusterThrottlersConfig_->ClusterLimits) {
        for (auto trafficType : TEnumTraits<EThrottlerTrafficType>::GetDomainValues()) {
            TThroughputThrottlerConfigPtr throttlerConfig;
            if (trafficType == EThrottlerTrafficType::Bandwidth) {
                throttlerConfig = clusterLimits->Bandwidth;
            }
            if (trafficType == EThrottlerTrafficType::Rps) {
                throttlerConfig = clusterLimits->Rps;
            }

            if (!throttlerConfig || !throttlerConfig->Limit) {
                YT_LOG_ERROR("Limit is not set for remote cluster (ClusterName: %v, TrafficType: %v)",
                    clusterName,
                    trafficType);
                continue;
            }

            auto throttlerId = ToThrottlerId(trafficType, TClusterName(clusterName));
            auto it = DistributedThrottlersHolder_.find(throttlerId);
            if (it == DistributedThrottlersHolder_.end()) {
                // This type of throttler hasn't been used yet.
                YT_LOG_DEBUG("Skip updating distributed throttler since it has been unused (ThrottlerId: %v)",
                    throttlerId);
                continue;
            }

            if (NYson::ConvertToYsonString(it->second.ThrottlerConfig) == NYson::ConvertToYsonString(throttlerConfig)) {
                YT_LOG_DEBUG("Skip updating distributed throttler since its config has not changed (ThrottlerId: %v, ThrottlerLimit: %v)",
                    throttlerId,
                    it->second.ThrottlerConfig->Limit);
                continue;
            }

            YT_LOG_INFO("Update distributed throttler (ThrottlerId: %v, OldThrottlerLimit: %v, NewThrottlerLimit: %v)",
                throttlerId,
                it->second.ThrottlerConfig->Limit,
                throttlerConfig->Limit);

            auto throttler = DistributedThrottlerFactory_->GetOrCreateThrottler(
                throttlerId,
                throttlerConfig);

            // Overwrite the old throttler by a new one.
            it->second = {std::move(throttler), std::move(throttlerConfig)};
        }
    }

    // Remove obsolete throttlers.
    for (auto it = DistributedThrottlersHolder_.begin(); it != DistributedThrottlersHolder_.end(); ) {
        if (!ClusterThrottlersConfig_->ClusterLimits.contains(it->first)) {
            it = DistributedThrottlersHolder_.erase(it);
        } else {
            ++it;
        }
    }
}

void TThrottlerManager::TryUpdateClusterThrottlersConfig()
{
    auto Logger = this->Logger.WithTag("UpdateClusterThrottlersConfigTag: %v", TGuid::Create());

    YT_LOG_DEBUG("Try update cluster throttlers config");

    auto future = GetClusterThrottlersYson(Client_);
    auto errorOrYson = NConcurrency::WaitFor(future);
    if (!errorOrYson.IsOK()) {
        YT_LOG_DEBUG("Failed to get cluster throttlers config: (Error: %v)", errorOrYson);
        return;
    }

    auto newConfigYson = errorOrYson.Value();
    YT_LOG_DEBUG("Got cluster throttlers config (Config: %v)",
        NYson::ConvertToYsonString(newConfigYson, NYson::EYsonFormat::Text));

    TClusterThrottlersConfigPtr newConfig;
    try {
        newConfig = ConvertTo<TClusterThrottlersConfigPtr>(newConfigYson);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to parse cluster throttlers config (Config: %v)",
            NYson::ConvertToYsonString(newConfigYson, NYson::EYsonFormat::Text));
        auto guard = Guard(Lock_);
        DistributedThrottlersHolder_.clear();
        DistributedThrottlerFactory_.Reset();
        return;
    }

    auto guard = Guard(Lock_);

    if (AreClusterThrottlersConfigsEqual(ClusterThrottlersConfig_, newConfig)) {
        YT_LOG_DEBUG("New cluster throttlers config is the same as the old one");
        return;
    }

    YT_LOG_DEBUG("New cluster throttlers config is different from the old one");

    ClusterThrottlersConfig_ = std::move(newConfig);

    ClusterThrottlersConfigUpdater_->SetPeriod(ClusterThrottlersConfig_->UpdatePeriod);

    if (NeedDistributedThrottlerFactory(ClusterThrottlersConfig_)) {
        if (DistributedThrottlerFactory_) {
            UpdateDistributedThrottlers();

            YT_LOG_INFO("Reconfigure distributed throttler factory");
            DistributedThrottlerFactory_->Reconfigure(ClusterThrottlersConfig_->DistributedThrottler);
        } else {
            DistributedThrottlersHolder_.clear();

            YT_LOG_INFO("Create distributed throttler factory");
            DistributedThrottlerFactory_ = CreateDistributedThrottlerFactory(
                ClusterThrottlersConfig_->DistributedThrottler,
                ChannelFactory_,
                Connection_,
                Invoker_,
                "/remote_cluster_throttlers_group",
                TMemberId(LocalAddress_),
                RpcServer_,
                // TODO(babenko): switch to std::string
                TString(LocalAddress_),
                this->Logger,
                Authenticator_,
                Profiler_.WithPrefix("/distributed_throttler"));
        }
    } else {
        DistributedThrottlersHolder_.clear();

        YT_LOG_INFO("Disable distributed throttler factory");
        DistributedThrottlerFactory_.Reset();
    }

    YT_LOG_DEBUG("Updated cluster throttlers config");
}

IThroughputThrottlerPtr TThrottlerManager::GetLocalThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType) const
{
    YT_VERIFY(static_cast<int>(kind) < std::ssize(Throttlers_));
    return Throttlers_[kind];
}

IThroughputThrottlerPtr TThrottlerManager::GetOrCreateDistributedThrottler(EExecNodeThrottlerKind, EThrottlerTrafficType trafficType, std::optional<TClusterName> remoteClusterName)
{
    if (!remoteClusterName || !DistributedThrottlerFactory_ || !ClusterThrottlersConfig_) {
        YT_LOG_DEBUG("Distributed throttler is not required (ClusterName: %v, HasDistributedThrottlerFactory: %v, HasClusterThrottlersConfig: %v)",
            remoteClusterName,
            !!DistributedThrottlerFactory_,
            !!ClusterThrottlersConfig_);
        return nullptr;
    }

    auto it = ClusterThrottlersConfig_->ClusterLimits.find(remoteClusterName->Underlying());
    if (it == ClusterThrottlersConfig_->ClusterLimits.end()) {
        YT_LOG_WARNING("Could not find remote cluster in cluster limits (ClusterName: %v)",
            remoteClusterName->Underlying());
        return nullptr;
    }

    const auto& [cluster, clusterLimit] = *it;

    TThroughputThrottlerConfigPtr throttlerConfig;
    if (trafficType == EThrottlerTrafficType::Bandwidth) {
        throttlerConfig = clusterLimit->Bandwidth;
    } else if (trafficType == EThrottlerTrafficType::Rps) {
        throttlerConfig = clusterLimit->Rps;
    }

    if (!throttlerConfig || !throttlerConfig->Limit) {
        YT_LOG_WARNING("Limit is not set for remote cluster (ClusterName: %v, TrafficType: %v)",
            cluster,
            trafficType);
        return nullptr;
    }

    auto throttlerId = ToThrottlerId(trafficType, TClusterName(cluster));
    auto distributedThrottlerIt = DistributedThrottlersHolder_.find(throttlerId);
    if (distributedThrottlerIt == DistributedThrottlersHolder_.end()) {
        YT_LOG_DEBUG("Creating new distributed throttler (ThrottlerId: %v, ThrottlerLimit: %v)",
            throttlerId,
            throttlerConfig->Limit);

        auto throttler = DistributedThrottlerFactory_->GetOrCreateThrottler(
            throttlerId,
            throttlerConfig);
        distributedThrottlerIt = DistributedThrottlersHolder_.insert({std::move(throttlerId), {std::move(throttler), std::move(throttlerConfig)}}).first;
    } else {
        YT_LOG_DEBUG("Getting distributed throttler (ThrottlerId: %v, ThrottlerLimit: %v)",
            throttlerId,
            distributedThrottlerIt->second.ThrottlerConfig->Limit);
    }

    return distributedThrottlerIt->second.Throttler;
}

TThrottlerManager::TThrottlerManager(
    NClusterNode::IBootstrap* bootstrap,
    TThrottlerManagerOptions options)
    : Bootstrap_(bootstrap)
    , Authenticator_(std::move(Bootstrap_->GetNativeAuthenticator()))
    , Connection_(Bootstrap_->GetConnection())
    , Client_(Connection_->CreateNativeClient(NApi::TClientOptions::FromUser(NSecurityClient::RootUserName)))
    , ChannelFactory_(Bootstrap_->GetConnection()->GetChannelFactory())
    , ClusterNodeConfig_(Bootstrap_->GetConfig())
    , Invoker_(Bootstrap_->GetControlInvoker())
    , RpcServer_(std::move(Bootstrap_->GetRpcServer()))
    , LocalAddress_(std::move(options.LocalAddress))
    , Profiler_(std::move(options.Profiler))
    , Logger(std::move(options.Logger))
    , ClusterThrottlersConfigUpdater_(New<TPeriodicExecutor>(
        Bootstrap_->GetControlInvoker(),
        BIND(&TThrottlerManager::TryUpdateClusterThrottlersConfig, MakeWeak(this)),
        // The period will be updated after the first config read.
        TDuration::Seconds(1)))
{
    if (ClusterNodeConfig_->EnableFairThrottler) {
        Throttlers_[EExecNodeThrottlerKind::JobIn] = Bootstrap_->GetInThrottler("job_in");
        Throttlers_[EExecNodeThrottlerKind::ArtifactCacheIn] = Bootstrap_->GetInThrottler("artifact_cache_in");
        Throttlers_[EExecNodeThrottlerKind::JobOut] = Bootstrap_->GetOutThrottler("job_out");
    } else {
        for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
            auto config = ClusterNodeConfig_->DataNode->Throttlers[GetDataNodeThrottlerKind(kind)];
            config = Bootstrap_->PatchRelativeNetworkThrottlerConfig(config);

            RawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
                std::move(config),
                ToString(kind),
                Logger,
                Profiler_.WithPrefix("/throttlers"));

            auto throttler = IThroughputThrottlerPtr(RawThrottlers_[kind]);
            if (kind == EExecNodeThrottlerKind::ArtifactCacheIn || kind == EExecNodeThrottlerKind::JobIn) {
                throttler = CreateCombinedThrottler({Bootstrap_->GetDefaultInThrottler(), throttler});
            } else if (kind == EExecNodeThrottlerKind::JobOut) {
                throttler = CreateCombinedThrottler({Bootstrap_->GetDefaultOutThrottler(), throttler});
            }
            Throttlers_[kind] = std::move(throttler);
        }
    }

    TryUpdateClusterThrottlersConfig();
    ClusterThrottlersConfigUpdater_->Start();
}

IThroughputThrottlerPtr TThrottlerManager::GetOrCreateThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType trafficType, std::optional<TClusterName> remoteClusterName)
{
    YT_LOG_DEBUG("Getting throttler (Kind: %v, TrafficType: %v, RemoteClusterName: %v)",
        kind,
        trafficType,
        remoteClusterName);

    IThroughputThrottlerPtr localThrottler, distributedThrottler;
    {
        auto guard = Guard(Lock_);

        localThrottler = GetLocalThrottler(kind, trafficType);
        distributedThrottler = GetOrCreateDistributedThrottler(kind, trafficType, remoteClusterName);
        if (!distributedThrottler) {
            return localThrottler;
        }
    }
    YT_VERIFY(localThrottler && distributedThrottler);

    YT_LOG_DEBUG("Creating combined throttler (Kind: %v, TrafficType: %v, RemoteClusterName: %v)",
        kind,
        trafficType,
        remoteClusterName);

    return CreateCombinedThrottler({std::move(localThrottler), std::move(distributedThrottler)});
}

void TThrottlerManager::Reconfigure(TClusterNodeDynamicConfigPtr dynamicConfig)
{
    auto guard = Guard(Lock_);

    if (!ClusterNodeConfig_->EnableFairThrottler) {
        for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
            auto dataNodeThrottlerKind = GetDataNodeThrottlerKind(kind);
            auto config = dynamicConfig->DataNode->Throttlers[dataNodeThrottlerKind]
                ? dynamicConfig->DataNode->Throttlers[dataNodeThrottlerKind]
                : ClusterNodeConfig_->DataNode->Throttlers[dataNodeThrottlerKind];
            config = Bootstrap_->PatchRelativeNetworkThrottlerConfig(config);
            RawThrottlers_[kind]->Reconfigure(std::move(config));
        }
    }
}

const TClusterThrottlersConfigPtr TThrottlerManager::GetClusterThrottlersConfig() const
{
    auto guard = Guard(Lock_);
    return ClusterThrottlersConfig_;
}

std::optional<THashMap<TClusterName, TThrottlerManager::TIncomingTrafficUtilization>> TThrottlerManager::GetClusterToIncomingTrafficUtilization(EThrottlerTrafficType trafficType) const
{
    std::shared_ptr<const THashMap<TThrottlerId, TThrottlerUsage>> throttlerToTotalUsage;
    {
        auto guard = Guard(Lock_);
        if (DistributedThrottlerFactory_) {
            throttlerToTotalUsage = DistributedThrottlerFactory_->TryGetThrottlerToGlobalUsage();
        }
    }

    if (!throttlerToTotalUsage) {
        // I am not the leader so return nullopt.
        return std::nullopt;
    }

    auto config = GetClusterThrottlersConfig();

    // I am the leader so return non null result.
    THashMap<TClusterName, TThrottlerManager::TIncomingTrafficUtilization> result;
    for (const auto& [throttlerId, totalUsage] : *throttlerToTotalUsage) {
        try {
            auto [type, clusterName] = FromThrottlerId(throttlerId);
            if (type != trafficType) {
                continue;
            }

            auto& trafficUtilization = result[clusterName];
            if (config) {
                // Calculate limit without ExtraLimitRatio.
                trafficUtilization.Limit = std::max(1.0, totalUsage.Limit) / (1 + config->DistributedThrottler->ExtraLimitRatio);
                trafficUtilization.RateLimitRatioHardThreshold = config->RateLimitRatioHardThreshold;
                trafficUtilization.RateLimitRatioSoftThreshold = config->RateLimitRatioSoftThreshold;
                trafficUtilization.MaxEstimatedTimeToReadPendingBytesThreshold = config->MaxEstimatedTimeToReadPendingBytesThreshold;
                trafficUtilization.MinEstimatedTimeToReadPendingBytesThreshold = config->MinEstimatedTimeToReadPendingBytesThreshold;
            } else {
                trafficUtilization.Limit = std::max(1.0, totalUsage.Limit);
            }

            trafficUtilization.Rate = totalUsage.Rate;
            trafficUtilization.PendingBytes = totalUsage.QueueTotalAmount;
            trafficUtilization.MaxEstimatedTimeToReadPendingBytes = totalUsage.MaxEstimatedOverdraftDuration;
            trafficUtilization.MinEstimatedTimeToReadPendingBytes = totalUsage.MinEstimatedOverdraftDuration;
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Unexpected throttler id %Qv", throttlerId);
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

IThrottlerManagerPtr CreateThrottlerManager(
    NClusterNode::IBootstrap* bootstrap,
    TThrottlerManagerOptions options)
{
    return New<TThrottlerManager>(
        std::move(bootstrap),
        std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
