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

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

using namespace NDataNode;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NDiscoveryClient;
using namespace NDistributedThrottler;

////////////////////////////////////////////////////////////////////////////////

class TThrottlerManager
    : public IThrottlerManager
{
public:
    TThrottlerManager(
        NClusterNode::IBootstrap* bootstrap,
        TThrottlerManagerOptions options);

    IThroughputThrottlerPtr GetOrCreateThrottler(EExecNodeThrottlerKind kind, EExecNodeThrottlerTraffic traffic, std::optional<TString> remoteClusterName) override;

    void Reconfigure(TClusterNodeDynamicConfigPtr dynamicConfig) override;

    TClusterThrottlersConfigPtr GetClusterThrottlersConfig() const override;

private:
    static constexpr auto UpdateClusterThrottlersConfigPeriod = TDuration::Minutes(5);

    NClusterNode::IBootstrap* const Bootstrap_ = nullptr;
    // Fields from bootstrap.
    const NRpc::IAuthenticatorPtr Authenticator_;
    const NApi::NNative::IConnectionPtr Connection_;
    const NApi::NNative::IClientPtr Client_;
    const NRpc::IChannelFactoryPtr ChannelFactory_;
    const TClusterNodeConfigPtr ClusterNodeConfig_;
    const IInvokerPtr Invoker_;
    const NRpc::IServerPtr RpcServer_;
    // Fields from manager options.
    const TString LocalAddress_;
    const NProfiling::TProfiler Profiler_;
    NLogging::TLogger Logger;

    const NConcurrency::TPeriodicExecutorPtr ClusterThrottlersConfigUpdater_;

    // The following members are protected by Lock_.
    TClusterThrottlersConfigPtr ClusterThrottlersConfig_;
    IDistributedThrottlerFactoryPtr DistributedThrottlerFactory_;
    THashMap<TString, IThroughputThrottlerPtr> DistributedThrottlersHolder_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    static EDataNodeThrottlerKind GetDataNodeThrottlerKind(EExecNodeThrottlerKind kind);

    static bool NeedDistributedThrottlerFactory(TClusterThrottlersConfigPtr config);

    void TryUpdateClusterThrottlersConfig();

    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr GetLocalThrottler(EExecNodeThrottlerKind kind) const;
    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr GetOrCreateDistributedThrottler(EExecNodeThrottlerTraffic traffic, std::optional<TString> remoteClusterName);
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

void TThrottlerManager::TryUpdateClusterThrottlersConfig()
{
    auto Logger = this->Logger.WithTag("UpdateClusterThrottlersConfigTag: %v", TGuid::Create());

    YT_LOG_DEBUG("Try update cluster throttlers config");

    auto newConfigYson = GetClusterThrottlersYson(Client_);
    if (!newConfigYson) {
        YT_LOG_DEBUG("Failed to get cluster throttlers config");
        return;
    }

    YT_LOG_DEBUG("Got cluster throttlers config (Config: %v)",
        NYson::ConvertToYsonString(*newConfigYson, NYson::EYsonFormat::Text));

    auto newConfig = MakeClusterThrottlersConfig(*newConfigYson);
    if (!newConfig) {
        YT_LOG_ERROR("Failed to make cluster throttlers config (Config: %v)",
            *newConfigYson);
        DistributedThrottlerFactory_.Reset();
        return;
    }

    {
        auto guard = Guard(Lock_);

        if (AreClusterThrottlersConfigsEqual(ClusterThrottlersConfig_, newConfig)) {
            YT_LOG_DEBUG("The new cluster throttlers config is the same as the old one");
            return;
        }

        YT_LOG_DEBUG("The new cluster throttlers config is different from the old one");

        ClusterThrottlersConfig_ = std::move(newConfig);

        if (NeedDistributedThrottlerFactory(ClusterThrottlersConfig_)) {
            if (DistributedThrottlerFactory_) {
                DistributedThrottlerFactory_.Reset();
                YT_LOG_INFO("Recreate distributed throttler factory");
            } else {
                YT_LOG_INFO("Create distributed throttler factory");
            }
            DistributedThrottlerFactory_ = CreateDistributedThrottlerFactory(
            ClusterThrottlersConfig_->DistributedThrottler,
            ChannelFactory_,
            Connection_,
            Invoker_,
            ClusterThrottlersConfig_->GroupId,
            LocalAddress_,
            RpcServer_,
            LocalAddress_,
            Logger,
            Authenticator_,
            Profiler_.WithPrefix("/distributed_throttler"));
        } else {
            YT_LOG_INFO("Disable distributed throttler factory");
            DistributedThrottlerFactory_.Reset();
        }
    }

    YT_LOG_DEBUG("Updated cluster throttlers config");
}

IThroughputThrottlerPtr TThrottlerManager::GetLocalThrottler(EExecNodeThrottlerKind kind) const
{
    YT_VERIFY(static_cast<int>(kind) < std::ssize(Throttlers_));
    return Throttlers_[kind];
}

IThroughputThrottlerPtr TThrottlerManager::GetOrCreateDistributedThrottler(EExecNodeThrottlerTraffic traffic, std::optional<TString> remoteClusterName)
{
    if (!remoteClusterName || !DistributedThrottlerFactory_ || !ClusterThrottlersConfig_) {
        YT_LOG_DEBUG("Distributed throttler is not required (ClusterName: %v, HasDistributedThrottlerFactory: %v, HasClusterThrottlersConfig: %v)",
            remoteClusterName,
            !!DistributedThrottlerFactory_,
            !!ClusterThrottlersConfig_);
        return nullptr;
    }

    auto it = ClusterThrottlersConfig_->ClusterLimits.find(*remoteClusterName);
    if (it == ClusterThrottlersConfig_->ClusterLimits.end()) {
        YT_LOG_WARNING("Couldn't find remote cluster in cluster limits (ClusterName: %v)", *remoteClusterName);
        return nullptr;
    }

    const auto& [cluster, clusterLimit] = *it;

    std::optional<i64> limit;
    if (traffic == EExecNodeThrottlerTraffic::Bandwidth) {
        limit = clusterLimit->Bandwidth->Limit;
    } else {
        limit = clusterLimit->Rps->Limit;
    }

    if (!limit) {
        YT_LOG_WARNING("Limit is not set for remote cluster (ClusterName: %v)", cluster);
        return nullptr;
    }

    auto throttlerId = Format("%v_%v", traffic, cluster);
    auto throttlerConfig = New<NConcurrency::TThroughputThrottlerConfig>();
    throttlerConfig->Limit = limit;

    YT_LOG_DEBUG("Getting distributed throttler (ThrottledId: %v, ThrottlerLimit: %v)",
        throttlerId,
        throttlerConfig->Limit);

    auto distributedThrottlerIt = DistributedThrottlersHolder_.find(throttlerId);
    if (distributedThrottlerIt == DistributedThrottlersHolder_.end()) {
        auto throttler = DistributedThrottlerFactory_->GetOrCreateThrottler(
            throttlerId,
            std::move(throttlerConfig));
        distributedThrottlerIt = DistributedThrottlersHolder_.insert({std::move(throttlerId), std::move(throttler)}).first;
    }

    return distributedThrottlerIt->second;
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
        UpdateClusterThrottlersConfigPeriod))
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

    ClusterThrottlersConfigUpdater_->Start();
}

IThroughputThrottlerPtr TThrottlerManager::GetOrCreateThrottler(EExecNodeThrottlerKind kind, EExecNodeThrottlerTraffic traffic, std::optional<TString> remoteClusterName)
{
    YT_LOG_DEBUG("Getting throttler (Kind: %v, Traffic: %v, RemoteClusterName: %v)",
        kind,
        traffic,
        remoteClusterName);

    IThroughputThrottlerPtr localThrottler, distributedThrottler;
    {
        auto guard = Guard(Lock_);

        localThrottler = GetLocalThrottler(kind);
        distributedThrottler = GetOrCreateDistributedThrottler(traffic, remoteClusterName);
        if (!distributedThrottler) {
            return localThrottler;
        }
    }
    YT_VERIFY(localThrottler && distributedThrottler);

    YT_LOG_DEBUG("Creating combined throttler (Kind: %v, Traffic: %v, RemoteClusterName: %v)",
        kind,
        traffic,
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

TClusterThrottlersConfigPtr TThrottlerManager::GetClusterThrottlersConfig() const
{
    auto guard = Guard(Lock_);
    return ClusterThrottlersConfig_;
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
