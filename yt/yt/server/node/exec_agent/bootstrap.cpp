#include "bootstrap.h"

#include "chunk_cache.h"
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

namespace NYT::NExecAgent {

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

    virtual void Initialize() override
    {
        GetDynamicConfigManager()
            ->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

        SlotManager_ = New<TSlotManager>(GetConfig()->ExecAgent->SlotManager, this);

        GpuManager_ = New<TGpuManager>(this, GetConfig()->ExecAgent->JobController->GpuManager);

        JobReporter_ = New<TJobReporter>(
            GetConfig()->ExecAgent->JobReporter,
            GetMasterConnection(),
            GetLocalDescriptor().GetDefaultAddress());

        MasterConnector_ = CreateMasterConnector(this);

        SchedulerConnector_ = New<TSchedulerConnector>(GetConfig()->ExecAgent->SchedulerConnector, this);

        BuildJobProxyConfigTemplate();

        ChunkCache_ = New<TChunkCache>(GetConfig()->DataNode, this);

        JobProxySolomonExporter_ = New<TSolomonExporter>(
            GetConfig()->ExecAgent->JobProxySolomonExporter,
            TProfileManager::Get()->GetInvoker(),
            New<TSolomonRegistry>());

        for (auto kind : TEnumTraits<EExecNodeThrottlerKind>::GetDomainValues()) {
            auto config = GetConfig()->DataNode->Throttlers[GetDataNodeThrottlerKind(kind)];
            config = ClusterNodeBootstrap_->PatchRelativeNetworkThrottlerConfig(config);

            RawThrottlers_[kind] = CreateNamedReconfigurableThroughputThrottler(
                std::move(config),
                ToString(kind),
                ExecAgentLogger,
                ExecAgentProfiler.WithPrefix("/throttlers"));

            auto throttler = IThroughputThrottlerPtr(RawThrottlers_[kind]);
            if (kind == EExecNodeThrottlerKind::ArtifactCacheIn || kind == EExecNodeThrottlerKind::JobIn) {
                throttler = CreateCombinedThrottler({GetTotalInThrottler(), throttler});
            } else if (kind == EExecNodeThrottlerKind::JobOut) {
                throttler = CreateCombinedThrottler({GetTotalOutThrottler(), throttler});
            }
            Throttlers_[kind] = throttler;
        }

        auto createExecJob = BIND([this] (
                TJobId jobId,
                TOperationId operationId,
                const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
                NJobTrackerClient::NProto::TJobSpec&& jobSpec) ->
                IJobPtr
            {
                return CreateUserJob(
                    jobId,
                    operationId,
                    resourceLimits,
                    std::move(jobSpec),
                    this);
            });
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::Map, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::PartitionMap, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::SortedMerge, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::OrderedMerge, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::UnorderedMerge, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::Partition, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::SimpleSort, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::IntermediateSort, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::FinalSort, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::SortedReduce, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::PartitionReduce, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::ReduceCombiner, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::RemoteCopy, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::OrderedMap, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::JoinReduce, createExecJob);
        GetJobController()->RegisterJobFactory(NJobAgent::EJobType::Vanilla, createExecJob);

        GetJobController()->AddHeartbeatProcessor<TSchedulerJobHeartbeatProcessor>(EObjectType::SchedulerJob, ClusterNodeBootstrap_);

        GetRpcServer()->RegisterService(CreateJobProberService(this));

        GetRpcServer()->RegisterService(CreateSupervisorService(this));

        SlotManager_->Initialize();
        ChunkCache_->Initialize();
    }

    virtual void Run() override
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

    virtual const TGpuManagerPtr& GetGpuManager() const override
    {
        return GpuManager_;
    }

    virtual const TSlotManagerPtr& GetSlotManager() const override
    {
        return SlotManager_;
    }

    virtual const TJobReporterPtr& GetJobReporter() const override
    {
        return JobReporter_;
    }

    virtual const TJobProxyConfigPtr& GetJobProxyConfigTemplate() const override
    {
        return JobProxyConfigTemplate_;
    }

    virtual const TChunkCachePtr& GetChunkCache() const override
    {
        return ChunkCache_;
    }

    virtual bool IsSimpleEnvironment() const override
    {
        return GetJobEnvironmentType() == EJobEnvironmentType::Simple;
    }

    virtual const IMasterConnectorPtr& GetMasterConnector() const override
    {
        return MasterConnector_;
    }

    virtual const IThroughputThrottlerPtr& GetThrottler(EExecNodeThrottlerKind kind) const override
    {
        return Throttlers_[kind];
    }

    virtual const TSolomonExporterPtr& GetJobProxySolomonExporter() const override
    {
        return JobProxySolomonExporter_;
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

        JobProxyConfigTemplate_->SupervisorRpcTimeout = GetConfig()->ExecAgent->SupervisorRpcTimeout;

        JobProxyConfigTemplate_->HeartbeatPeriod = GetConfig()->ExecAgent->JobProxyHeartbeatPeriod;

        JobProxyConfigTemplate_->JobEnvironment = GetConfig()->ExecAgent->SlotManager->JobEnvironment;

        JobProxyConfigTemplate_->Logging = GetConfig()->ExecAgent->JobProxyLogging;
        JobProxyConfigTemplate_->Jaeger = GetConfig()->ExecAgent->JobProxyJaeger;
        JobProxyConfigTemplate_->StderrPath = GetConfig()->ExecAgent->JobProxyStderrPath;
        JobProxyConfigTemplate_->TestRootFS = GetConfig()->ExecAgent->TestRootFS;

        JobProxyConfigTemplate_->CoreWatcher = GetConfig()->ExecAgent->CoreWatcher;

        JobProxyConfigTemplate_->TestPollJobShell = GetConfig()->ExecAgent->TestPollJobShell;

        JobProxyConfigTemplate_->DoNotSetUserId = GetConfig()->ExecAgent->DoNotSetUserId;
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

} // namespace NYT::NExecAgent
