#include "bootstrap.h"

#include "chunk_cache.h"
#include "controller_agent_connector.h"
#include "exec_node_admin_service.h"
#include "gpu_manager.h"
#include "job.h"
#include "job_heartbeat_processor.h"
#include "job_prober_service.h"
#include "master_connector.h"
#include "private.h"
#include "scheduler_connector.h"
#include "slot_manager.h"
#include "supervisor_service.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>
#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/bootstrap.h>
#include <yt/yt/server/node/data_node/ytree_integration.h>

#include <yt/yt/server/node/job_agent/job_controller.h>

#include <yt/yt/server/lib/job_agent/job_reporter.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NClusterNode;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NDataNode;
using namespace NJobAgent;
using namespace NJobProxy;
using namespace NNodeTrackerClient;
using namespace NProfiling;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
    , public TBootstrapBase
{
public:
    explicit TBootstrap(NClusterNode::IBootstrap* bootstrap)
        : TBootstrapBase(bootstrap)
        , ClusterNodeBootstrap_(bootstrap)
    { }

    void Initialize() override
    {
        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

        SlotManager_ = New<TSlotManager>(GetConfig()->ExecNode->SlotManager, this);

        GpuManager_ = New<TGpuManager>(this, GetConfig()->ExecNode->JobController->GpuManager);

        JobReporter_ = New<TJobReporter>(
            GetConfig()->ExecNode->JobReporter,
            GetMasterConnection(),
            GetLocalDescriptor().GetDefaultAddress());

        MasterConnector_ = CreateMasterConnector(this);

        SchedulerConnector_ = New<TSchedulerConnector>(GetConfig()->ExecNode->SchedulerConnector, this);

        ControllerAgentConnectorPtr_ = New<TControllerAgentConnector>(GetConfig()->ExecNode->ControllerAgentConnector, this);

        BuildJobProxyConfigTemplate();

        ChunkCache_ = New<TChunkCache>(GetConfig()->DataNode, this);

        JobProxySolomonExporter_ = New<TSolomonExporter>(
            GetConfig()->ExecNode->JobProxySolomonExporter,
            TProfileManager::Get()->GetInvoker(),
            New<TSolomonRegistry>());

        for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
            auto config = GetConfig()->DataNode->Throttlers[GetDataNodeThrottlerKind(kind)];
            config = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(config);

            RawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
                std::move(config),
                ToString(kind),
                ExecNodeLogger,
                ExecNodeProfiler.WithPrefix("/throttlers"));

            auto throttler = IThroughputThrottlerPtr(RawThrottlers_[kind]);
            if (kind == EExecNodeThrottlerKind::ArtifactCacheIn || kind == EExecNodeThrottlerKind::JobIn) {
                throttler = CreateCombinedThrottler({GetTotalInThrottler(), throttler});
            } else if (kind == EExecNodeThrottlerKind::JobOut) {
                throttler = CreateCombinedThrottler({GetTotalOutThrottler(), throttler});
            }
            Throttlers_[kind] = throttler;
        }

        auto createSchedulerJob = BIND([this] (
            TJobId jobId,
            TOperationId operationId,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec,
            const TControllerAgentDescriptor& agentDescriptor) ->
            NJobAgent::IJobPtr
        {
            return CreateSchedulerJob(
                jobId,
                operationId,
                resourceLimits,
                std::move(jobSpec),
                this,
                agentDescriptor);
        });

        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::Map, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::PartitionMap, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::SortedMerge, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::OrderedMerge, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::UnorderedMerge, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::Partition, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::SimpleSort, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::IntermediateSort, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::FinalSort, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::SortedReduce, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::PartitionReduce, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::ReduceCombiner, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::RemoteCopy, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::OrderedMap, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::JoinReduce, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::Vanilla, createSchedulerJob);
        GetJobController()->RegisterSchedulerJobFactory(NJobAgent::EJobType::ShallowMerge, createSchedulerJob);

        GetJobController()->AddHeartbeatProcessor<TSchedulerJobHeartbeatProcessor>(EObjectType::SchedulerJob, ClusterNodeBootstrap_);

        GetRpcServer()->RegisterService(CreateJobProberService(this));

        GetRpcServer()->RegisterService(CreateSupervisorService(this));

        GetRpcServer()->RegisterService(CreateExecNodeAdminService(this));

        SlotManager_->Initialize();
        ChunkCache_->Initialize();
    }

    void Run() override
    {
        SetNodeByYPath(
            GetOrchidRoot(),
            "/cached_chunks",
            CreateVirtualNode(CreateCachedChunkMapService(ChunkCache_)
                ->Via(GetControlInvoker())));
        SetNodeByYPath(
            GetOrchidRoot(),
            "/job_proxy_sensors",
            CreateVirtualNode(JobProxySolomonExporter_->GetSensorService()));

        JobProxySolomonExporter_->Register("/solomon/job_proxy", GetHttpServer());
        JobProxySolomonExporter_->Start();

        MasterConnector_->Initialize();

        SchedulerConnector_->Start();
    }

    const TGpuManagerPtr& GetGpuManager() const override
    {
        return GpuManager_;
    }

    const TSlotManagerPtr& GetSlotManager() const override
    {
        return SlotManager_;
    }

    const TJobReporterPtr& GetJobReporter() const override
    {
        return JobReporter_;
    }

    const TJobProxyConfigPtr& GetJobProxyConfigTemplate() const override
    {
        return JobProxyConfigTemplate_;
    }

    const TChunkCachePtr& GetChunkCache() const override
    {
        return ChunkCache_;
    }

    bool IsSimpleEnvironment() const override
    {
        return GetJobEnvironmentType() == EJobEnvironmentType::Simple;
    }

    const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    const IThroughputThrottlerPtr& GetThrottler(EExecNodeThrottlerKind kind) const override
    {
        return Throttlers_[kind];
    }

    const TSolomonExporterPtr& GetJobProxySolomonExporter() const override
    {
        return JobProxySolomonExporter_;
    }

    virtual const TControllerAgentConnectorPtr& GetControllerAgentConnector() const override
    {
        return ControllerAgentConnectorPtr_;
    }

private:
    NClusterNode::IBootstrap* const ClusterNodeBootstrap_;

    TSlotManagerPtr SlotManager_;

    TGpuManagerPtr GpuManager_;

    TJobReporterPtr JobReporter_;

    TJobProxyConfigPtr JobProxyConfigTemplate_;

    TChunkCachePtr ChunkCache_;

    IMasterConnectorPtr MasterConnector_;

    TSchedulerConnectorPtr SchedulerConnector_;

    TSolomonExporterPtr JobProxySolomonExporter_;

    TEnumIndexedVector<EExecNodeThrottlerKind, IReconfigurableThroughputThrottlerPtr> RawThrottlers_;
    TEnumIndexedVector<EExecNodeThrottlerKind, IThroughputThrottlerPtr> Throttlers_;

    TControllerAgentConnectorPtr ControllerAgentConnectorPtr_;

    void BuildJobProxyConfigTemplate()
    {
        auto localRpcAddresses = GetLocalAddresses(GetConfig()->Addresses, GetConfig()->RpcPort);
        auto localAddress = GetDefaultAddress(localRpcAddresses);

        JobProxyConfigTemplate_ = New<NJobProxy::TJobProxyConfig>();

        // Singletons.
        JobProxyConfigTemplate_->FiberStackPoolSizes = GetConfig()->FiberStackPoolSizes;
        JobProxyConfigTemplate_->AddressResolver = GetConfig()->AddressResolver;
        JobProxyConfigTemplate_->RpcDispatcher = GetConfig()->RpcDispatcher;
        JobProxyConfigTemplate_->ChunkClientDispatcher = GetConfig()->ChunkClientDispatcher;
        JobProxyConfigTemplate_->JobThrottler = GetConfig()->JobThrottler;

        JobProxyConfigTemplate_->ClusterConnection = CloneYsonSerializable(GetConfig()->ClusterConnection);
        JobProxyConfigTemplate_->ClusterConnection->MasterCellDirectorySynchronizer->RetryPeriod = std::nullopt;

        auto patchMasterConnectionConfig = [&] (const NNative::TMasterConnectionConfigPtr& config) {
            config->Addresses = {localAddress};
            config->Endpoints = nullptr;
            if (config->RetryTimeout && *config->RetryTimeout > config->RpcTimeout) {
                config->RpcTimeout = *config->RetryTimeout;
            }
            config->RetryTimeout = std::nullopt;
            config->RetryAttempts = 1;
        };

        patchMasterConnectionConfig(JobProxyConfigTemplate_->ClusterConnection->PrimaryMaster);
        for (const auto& config : JobProxyConfigTemplate_->ClusterConnection->SecondaryMasters) {
            patchMasterConnectionConfig(config);
        }
        if (JobProxyConfigTemplate_->ClusterConnection->MasterCache) {
            patchMasterConnectionConfig(JobProxyConfigTemplate_->ClusterConnection->MasterCache);
            JobProxyConfigTemplate_->ClusterConnection->MasterCache->EnableMasterCacheDiscovery = false;
        }

        JobProxyConfigTemplate_->SupervisorConnection = New<NYT::NBus::TTcpBusClientConfig>();

        JobProxyConfigTemplate_->SupervisorConnection->Address = localAddress;

        JobProxyConfigTemplate_->SupervisorRpcTimeout = GetConfig()->ExecNode->SupervisorRpcTimeout;

        JobProxyConfigTemplate_->HeartbeatPeriod = GetConfig()->ExecNode->JobProxyHeartbeatPeriod;

        JobProxyConfigTemplate_->JobEnvironment = GetConfig()->ExecNode->SlotManager->JobEnvironment;

        JobProxyConfigTemplate_->Logging = GetConfig()->ExecNode->JobProxyLogging;
        JobProxyConfigTemplate_->Jaeger = GetConfig()->ExecNode->JobProxyJaeger;
        JobProxyConfigTemplate_->StderrPath = GetConfig()->ExecNode->JobProxyStderrPath;
        JobProxyConfigTemplate_->TestRootFS = GetConfig()->ExecNode->TestRootFS;

        JobProxyConfigTemplate_->CoreWatcher = GetConfig()->ExecNode->CoreWatcher;

        JobProxyConfigTemplate_->TestPollJobShell = GetConfig()->ExecNode->TestPollJobShell;

        JobProxyConfigTemplate_->DoNotSetUserId = GetConfig()->ExecNode->DoNotSetUserId;
    }

    void OnDynamicConfigChanged(
        const TClusterNodeDynamicConfigPtr& /*oldConfig*/,
        const TClusterNodeDynamicConfigPtr& newConfig)
    {
        for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
            auto dataNodeThrottlerKind = GetDataNodeThrottlerKind(kind);
            auto config = newConfig->DataNode->Throttlers[dataNodeThrottlerKind]
                ? newConfig->DataNode->Throttlers[dataNodeThrottlerKind]
                : GetConfig()->DataNode->Throttlers[dataNodeThrottlerKind];
            config = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(config);
            RawThrottlers_[kind]->Reconfigure(std::move(config));
        }
    }

    static EDataNodeThrottlerKind GetDataNodeThrottlerKind(EExecNodeThrottlerKind kind)
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
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(NClusterNode::IBootstrap* bootstrap)
{
    return std::make_unique<TBootstrap>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
