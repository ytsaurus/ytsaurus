#include "throttler_manager.h"
#include "bootstrap.h"
#include "master_connector.h"
#include "public.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/master_connector.h>

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

    ~TThrottlerManager();

    TFuture<void> Start() override;

    IThroughputThrottlerPtr GetOrCreateThrottler(
        EExecNodeThrottlerKind kind,
        EThrottlerTrafficType trafficType,
        TClusterName clusterName) override;

    void Reconfigure(TClusterNodeDynamicConfigPtr dynamicConfig) override;

    const TClusterThrottlersConfigPtr GetClusterThrottlersConfig() const override;

    std::optional<THashMap<TClusterName, TIncomingTrafficUtilization>> GetClusterToIncomingTrafficUtilization(
        EThrottlerTrafficType trafficType) const override;

    std::optional<THashMap<TClusterName, bool>> GetClusterToIncomingTrafficAvailability(
        EThrottlerTrafficType trafficType) override;

private:
    static constexpr TDuration DefaultConfigUpdatePeriod = TDuration::Seconds(10);

    NClusterNode::IBootstrap* const Bootstrap_;
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
    const NLogging::TLogger Logger;

    const TPeriodicExecutorPtr ClusterThrottlersConfigUpdater_;
    const TPromise<void> ClusterThrottlersConfigInitializedPromise_ = NewPromise<void>();

    const TCallback<void(bool)> JobsDisabledByMasterUpdatedCallback_;
    const TCallback<void()> MasterConnectedCallback_;
    const TCallback<void()> MasterDisconnectedCallback_;

    // The following members are protected by Lock_.
    bool MasterConnected_ = false;
    bool JobsDisabledByMaster_ = false;
    TClusterThrottlersConfigPtr ClusterThrottlersConfig_;
    IDistributedThrottlerFactoryPtr DistributedThrottlerFactory_;
    struct TThroughputThrottlerData
    {
        IThroughputThrottlerPtr Throttler;
        TThroughputThrottlerConfigPtr ThrottlerConfig;
    };
    std::map<TThrottlerId, TThroughputThrottlerData> DistributedThrottlersHolder_;
    std::map<TThrottlerId, NProfiling::TGauge> NetworkAvailabilityGauges_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedArray<EExecNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);

    static EDataNodeThrottlerKind GetDataNodeThrottlerKind(EExecNodeThrottlerKind kind);

    static bool NeedDistributedThrottlerFactory(TClusterThrottlersConfigPtr config);

    static TThrottlerId ToThrottlerId(EThrottlerTrafficType trafficType, const TClusterName& clusterName);
    static std::pair<EThrottlerTrafficType, TClusterName> FromThrottlerId(const TThrottlerId& throttlerId);

    void OnMasterConnected();
    void OnMasterDisconnected();

    void UpdateJobsDisabledByMaster(bool jobsDisabledByMaster);

    void TryUpdateClusterThrottlersConfig();
    //! Lock_ has to be taken prior to calling this method.
    void UpdateClusterLimits(const THashMap<std::string, TClusterLimitsConfigPtr>& clusterLimits);
    //! Lock_ has to be taken prior to calling this method.
    void UpdateDistributedThrottlers();
    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr GetLocalThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType trafficType) const;
    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr GetOrCreateDistributedThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType trafficType, TClusterName clusterName);
    //! Lock_ has to be taken prior to calling this method.
    IThroughputThrottlerPtr DoGetOrCreateThrottler(
        EExecNodeThrottlerKind kind,
        EThrottlerTrafficType trafficType,
        NScheduler::TClusterName clusterName);
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
        if (throttlerId.starts_with(trafficTypeString)) {
            return {trafficType, TClusterName(throttlerId.substr(trafficTypeString.size()))};
        }
    }
    THROW_ERROR_EXCEPTION("Invalid throttler id %Qv", throttlerId);
}

void TThrottlerManager::OnMasterConnected()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Update cluster throttlers on master connect");

    {
        auto guard = Guard(Lock_);
        MasterConnected_ = true;
    }

    TryUpdateClusterThrottlersConfig();
}

void TThrottlerManager::OnMasterDisconnected()
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    YT_LOG_INFO("Disable cluster throttlers on master disconnect");

    {
        auto guard = Guard(Lock_);
        MasterConnected_ = false;
        ClusterThrottlersConfig_.Reset();
        DistributedThrottlersHolder_.clear();
        DistributedThrottlerFactory_.Reset();
    }
}

void TThrottlerManager::UpdateJobsDisabledByMaster(bool jobsDisabledByMaster)
{
    YT_ASSERT_THREAD_AFFINITY_ANY();

    {
        auto guard = Guard(Lock_);

        YT_LOG_DEBUG("Update JobsDisabledByMaster (OldJobsDisabledByMaster: %v, NewJobsDisabledByMaster: %v)",
            JobsDisabledByMaster_,
            jobsDisabledByMaster);

        if (!JobsDisabledByMaster_ && jobsDisabledByMaster) {
            YT_LOG_INFO("Disable cluster throttlers on jobs disabled by master");

            ClusterThrottlersConfig_.Reset();
            DistributedThrottlersHolder_.clear();
            DistributedThrottlerFactory_.Reset();
        }

        JobsDisabledByMaster_ = jobsDisabledByMaster;
    }
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

    auto clearState = [&] {
        auto guard = Guard(Lock_);
        ClusterThrottlersConfig_.Reset();
        DistributedThrottlersHolder_.clear();
        DistributedThrottlerFactory_.Reset();
        ClusterThrottlersConfigUpdater_->SetPeriod(DefaultConfigUpdatePeriod);
    };

    auto future = GetClusterThrottlersYson(Client_);
    auto errorOrYson = WaitFor(future);
    if (errorOrYson.FindMatching(NYTree::EErrorCode::ResolveError)) {
        YT_LOG_DEBUG(errorOrYson, "Cluster throttlers are not configured");
        clearState();
        ClusterThrottlersConfigInitializedPromise_.TrySet();
        return;
    }

    if (!errorOrYson.IsOK()) {
        YT_LOG_WARNING(errorOrYson, "Failed to get cluster throttlers config");
        return;
    }

    const auto& newConfigYson = errorOrYson.Value();
    YT_LOG_DEBUG("Got cluster throttlers config (Config: %v)",
        NYson::ConvertToYsonString(newConfigYson, NYson::EYsonFormat::Text));

    TClusterThrottlersConfigPtr newConfig;
    try {
        newConfig = ConvertTo<TClusterThrottlersConfigPtr>(newConfigYson);
    } catch (const std::exception& ex) {
        YT_LOG_ERROR(ex, "Failed to parse cluster throttlers config (Config: %v)",
            NYson::ConvertToYsonString(newConfigYson, NYson::EYsonFormat::Text));

        clearState();

        // NB: Still regard the manager to be initialized, albeit unsuccessfully.
        ClusterThrottlersConfigInitializedPromise_.TrySet();
        return;
    }

    {
        auto guard = Guard(Lock_);

        if (JobsDisabledByMaster_) {
            YT_LOG_DEBUG("Skip updating cluster throttlers config since jobs are disabled by master");
            return;
        }

        if (!MasterConnected_) {
            YT_LOG_DEBUG("Skip updating cluster throttlers config since master is disconnected");
            return;
        }

        if (AreClusterThrottlersConfigsEqual(ClusterThrottlersConfig_, newConfig)) {
            YT_LOG_DEBUG("New cluster throttlers config is the same as the old one");
            return;
        }

        YT_LOG_INFO("New cluster throttlers config is different from the old one");

        ClusterThrottlersConfig_ = std::move(newConfig);

        std::optional<i64> memberPriority;
        if (!ClusterThrottlersConfig_->LeaderNodeTagFilter.IsSatisfiedBy(Bootstrap_->GetLocalDescriptor().GetTags())) {
            YT_LOG_INFO("Leader node tag filter is not satisfied, set the lowest member priority (LeaderNodeTagFilter: %v, NodeTags: %v)",
                ClusterThrottlersConfig_->LeaderNodeTagFilter,
                Bootstrap_->GetLocalDescriptor().GetTags());

            // Set the lowest member priority. The higher the value, the lower the priority.
            memberPriority = std::numeric_limits<i64>::max();
        } else {
            YT_LOG_INFO("Leader node tag filter is satisfied (LeaderNodeTagFilter: %v, NodeTags: %v)",
                ClusterThrottlersConfig_->LeaderNodeTagFilter,
                Bootstrap_->GetLocalDescriptor().GetTags());
        }

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
                    LocalAddress_,
                    this->Logger,
                    Authenticator_,
                    Profiler_.WithPrefix("/distributed_throttler"),
                    std::move(memberPriority));
            }

            UpdateClusterLimits(ClusterThrottlersConfig_->ClusterLimits);
        } else {
            DistributedThrottlersHolder_.clear();

            YT_LOG_INFO("Disable distributed throttler factory");
            DistributedThrottlerFactory_.Reset();
        }

        YT_LOG_INFO("Updated cluster throttlers config");
    }

    ClusterThrottlersConfigInitializedPromise_.TrySet();
}

void TThrottlerManager::UpdateClusterLimits(const THashMap<std::string, TClusterLimitsConfigPtr>& clusterLimits)
{
    YT_VERIFY(ClusterThrottlersConfig_);

    THashMap<TThrottlerId, std::optional<double>> throttlerIdToTotalLimit;
    for (const auto& [clusterName, throttlerConfig] : clusterLimits) {
        if (!throttlerConfig) {
            continue;
        }

        EThrottlerTrafficType trafficType{};
        if (throttlerConfig->Bandwidth) {
            trafficType = EThrottlerTrafficType::Bandwidth;
        } else if (throttlerConfig->Rps) {
            trafficType = EThrottlerTrafficType::Rps;
        } else {
            YT_LOG_WARNING("Unexpected traffic type in cluster limits config (ClusterName: %v)",
                clusterName);
            continue;
        }

        auto throttlerId = ToThrottlerId(trafficType, TClusterName(clusterName));
        EmplaceOrCrash(throttlerIdToTotalLimit, throttlerId, throttlerConfig->Bandwidth->Limit);
    }
    DistributedThrottlerFactory_->UpdateTotalLimits(throttlerIdToTotalLimit);

    // Go through cluster throttlers to make this member appear in discovery service.
    // To keep things simple we touch throttlers at every cluster limits update.
    for (const auto& [throttlerId, _] : throttlerIdToTotalLimit) {
        const auto& [trafficType, clusterName] = FromThrottlerId(throttlerId);
        auto throttler = DoGetOrCreateThrottler(EExecNodeThrottlerKind::JobIn, trafficType, clusterName);
        throttler->TryAcquire(0);
    }
}

IThroughputThrottlerPtr TThrottlerManager::GetLocalThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType) const
{
    YT_VERIFY(static_cast<int>(kind) < std::ssize(Throttlers_));
    return Throttlers_[kind];
}

IThroughputThrottlerPtr TThrottlerManager::GetOrCreateDistributedThrottler(EExecNodeThrottlerKind, EThrottlerTrafficType trafficType, TClusterName clusterName)
{
    if (IsLocal(clusterName) || !DistributedThrottlerFactory_ || !ClusterThrottlersConfig_) {
        YT_LOG_DEBUG("Distributed throttler is not required (ClusterName: %v, HasDistributedThrottlerFactory: %v, HasClusterThrottlersConfig: %v)",
            clusterName,
            !!DistributedThrottlerFactory_,
            !!ClusterThrottlersConfig_);
        return nullptr;
    }

    auto it = ClusterThrottlersConfig_->ClusterLimits.find(*clusterName.Underlying());
    if (it == ClusterThrottlersConfig_->ClusterLimits.end()) {
        YT_LOG_WARNING("Could not find remote cluster in cluster limits (ClusterName: %v)",
            clusterName);
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

IThroughputThrottlerPtr TThrottlerManager::DoGetOrCreateThrottler(
    EExecNodeThrottlerKind kind,
    EThrottlerTrafficType trafficType,
    TClusterName clusterName)
{
    auto Logger = this->Logger.WithTag("Kind: %v, TrafficType: %v, ClusterName: %v",
        kind,
        trafficType,
        clusterName);

    IThroughputThrottlerPtr localThrottler;
    IThroughputThrottlerPtr distributedThrottler;
    {
        localThrottler = GetLocalThrottler(kind, trafficType);
        distributedThrottler = GetOrCreateDistributedThrottler(kind, trafficType, clusterName);
        if (!distributedThrottler) {
            YT_LOG_DEBUG("Distributed throttler is missing; falling back to local throttler (MasterConnected: %v, JobsDisabledByMaster: %v)",
                MasterConnected_,
                JobsDisabledByMaster_);
            return localThrottler;
        }
    }
    YT_VERIFY(localThrottler && distributedThrottler);

    YT_LOG_DEBUG("Creating combined throttler");
    return CreateCombinedThrottler({std::move(localThrottler), std::move(distributedThrottler)});
}

////////////////////////////////////////////////////////////////////////////////

TThrottlerManager::TThrottlerManager(
    NClusterNode::IBootstrap* bootstrap,
    TThrottlerManagerOptions options)
    : Bootstrap_(bootstrap)
    , Authenticator_(std::move(Bootstrap_->GetNativeAuthenticator()))
    , Connection_(Bootstrap_->GetConnection())
    , Client_(Connection_->CreateNativeClient(NApi::NNative::TClientOptions::FromUser(NSecurityClient::RootUserName)))
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
        // Default period will be updated once config has been retrieved.
        DefaultConfigUpdatePeriod))
    , JobsDisabledByMasterUpdatedCallback_(BIND_NO_PROPAGATE(&TThrottlerManager::UpdateJobsDisabledByMaster, MakeWeak(this)))
    , MasterConnectedCallback_(BIND_NO_PROPAGATE(&TThrottlerManager::OnMasterConnected, MakeWeak(this)))
    , MasterDisconnectedCallback_(BIND_NO_PROPAGATE(&TThrottlerManager::OnMasterDisconnected, MakeWeak(this)))
{
    YT_LOG_INFO("Constructing throttler manager");

    if (ClusterNodeConfig_->EnableFairThrottler) {
        Throttlers_[EExecNodeThrottlerKind::JobIn] = Bootstrap_->CreateInThrottler("job_in");
        Throttlers_[EExecNodeThrottlerKind::ArtifactCacheIn] = Bootstrap_->CreateInThrottler("artifact_cache_in");
        Throttlers_[EExecNodeThrottlerKind::JobOut] = Bootstrap_->CreateOutThrottler("job_out");
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

    const auto& masterConnector = Bootstrap_->GetExecNodeBootstrap()->GetMasterConnector();
    masterConnector->SubscribeJobsDisabledByMasterUpdated(JobsDisabledByMasterUpdatedCallback_);

    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
    clusterNodeMasterConnector->SubscribeMasterConnected(MasterConnectedCallback_);
    clusterNodeMasterConnector->SubscribeMasterDisconnected(MasterDisconnectedCallback_);
}

TThrottlerManager::~TThrottlerManager()
{
    YT_LOG_INFO("Destructing throttler manager");

    const auto& masterConnector = Bootstrap_->GetExecNodeBootstrap()->GetMasterConnector();
    masterConnector->UnsubscribeJobsDisabledByMasterUpdated(JobsDisabledByMasterUpdatedCallback_);

    const auto& clusterNodeMasterConnector = Bootstrap_->GetClusterNodeBootstrap()->GetMasterConnector();
    clusterNodeMasterConnector->UnsubscribeMasterConnected(MasterConnectedCallback_);
    clusterNodeMasterConnector->UnsubscribeMasterDisconnected(MasterDisconnectedCallback_);
}

TFuture<void> TThrottlerManager::Start()
{
    ClusterThrottlersConfigUpdater_->Start();
    ClusterThrottlersConfigUpdater_->ScheduleOutOfBand();

    return ClusterThrottlersConfigInitializedPromise_.ToFuture();
}

IThroughputThrottlerPtr TThrottlerManager::GetOrCreateThrottler(EExecNodeThrottlerKind kind, EThrottlerTrafficType trafficType, TClusterName clusterName)
{
    auto guard = Guard(Lock_);
    return DoGetOrCreateThrottler(kind, trafficType, clusterName);
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
    TIntrusivePtr<const TThrottlerToGlobalUsage> throttlerToTotalUsage;
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

std::optional<THashMap<TClusterName, bool>> TThrottlerManager::GetClusterToIncomingTrafficAvailability(EThrottlerTrafficType trafficType)
{
    auto clusterTrafficUtilization = GetClusterToIncomingTrafficUtilization(trafficType);
    if (!clusterTrafficUtilization) {
        auto guard = Guard(Lock_);
        // Leave monitoring sensors only on leader.
        NetworkAvailabilityGauges_.clear();
        return std::nullopt;
    }

    THashMap<TClusterName, bool> result;
    auto addClusterToResult = [&] (const TClusterName& clusterName, bool isAvailable) {
        result[clusterName] = isAvailable;
        auto throttlerId = ToThrottlerId(trafficType, clusterName);
        if (!NetworkAvailabilityGauges_.contains(throttlerId)) {
            NetworkAvailabilityGauges_[throttlerId] = Profiler_
                .WithPrefix("/distributed_throttler")
                .WithTag("throttler_id", throttlerId)
                .Gauge("/network_availability");
        }
        NetworkAvailabilityGauges_[throttlerId].Update(isAvailable);
    };

    auto guard = Guard(Lock_);
    for (const auto& [clusterName, trafficUtilization] : *clusterTrafficUtilization) {
        if (trafficUtilization.Limit <= 0) {
            YT_LOG_DEBUG("Skip adding cluster network bandwidth availability to controller agent heartbeat request because of unexpected limit (ClusterName: %v, Limit: %v)",
                clusterName,
                trafficUtilization.Limit);
            continue;
        }

        auto rateLimitRatio = trafficUtilization.Rate / trafficUtilization.Limit;
        auto isAvailable = true;

        if (trafficUtilization.RateLimitRatioHardThreshold < rateLimitRatio) {
            // Network usage is higher than the available limit.
            isAvailable = false;
        } else if (trafficUtilization.MinEstimatedTimeToReadPendingBytesThreshold < trafficUtilization.MinEstimatedTimeToReadPendingBytes) {
            // There is a queue of pending read requests on every exe node.
            isAvailable = false;
        } else if (trafficUtilization.RateLimitRatioSoftThreshold < rateLimitRatio) {
            // Network usage is above threshold.
            if (trafficUtilization.MaxEstimatedTimeToReadPendingBytesThreshold < trafficUtilization.MaxEstimatedTimeToReadPendingBytes) {
                // There is a big queue of pending read requests on some exe node.
                isAvailable = false;
            }
        }

        addClusterToResult(clusterName, isAvailable);

        YT_LOG_DEBUG(
            "Add cluster network bandwidth availability to controller agent heartbeat request "
            "(ClusterName: %v, Rate: %v, Limit: %v, "
            "RateLimitRatio: %v, RateLimitRatioHardThreshold: %v, RateLimitRatioSoftThreshold: %v, "
            "MaxEstimatedTimeToReadPendingBytes: %v, MaxEstimatedTimeToReadPendingBytesThreshold: %v, "
            "MinEstimatedTimeToReadPendingBytes: %v, MinEstimatedTimeToReadPendingBytesThreshold: %v, "
            "PendingBytes: %v, IsAvailable: %v)",
            clusterName,
            trafficUtilization.Rate,
            trafficUtilization.Limit,
            rateLimitRatio,
            trafficUtilization.RateLimitRatioHardThreshold,
            trafficUtilization.RateLimitRatioSoftThreshold,
            trafficUtilization.MaxEstimatedTimeToReadPendingBytes,
            trafficUtilization.MaxEstimatedTimeToReadPendingBytesThreshold,
            trafficUtilization.MinEstimatedTimeToReadPendingBytes,
            trafficUtilization.MinEstimatedTimeToReadPendingBytesThreshold,
            trafficUtilization.PendingBytes,
            isAvailable);
    }

    // Lock is already taken.
    const auto& config = ClusterThrottlersConfig_;
    if (config && config->RateLimitRatioHardThreshold < 0) {
        // Make all clusters unavailable.
        for (const auto& [clusterName, clusterLimits] : config->ClusterLimits) {
            bool isAvailable = false;
            addClusterToResult(TClusterName{clusterName}, isAvailable);

            YT_LOG_DEBUG(
                "Add cluster network bandwidth availability to controller agent heartbeat request "
                "(ClusterName: %v, IsAvailable: %v)",
                clusterName,
                isAvailable);
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
