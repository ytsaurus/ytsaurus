#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <core/misc/address.h>
#include <core/misc/ref_counted_tracker.h>

#include <core/concurrency/action_queue.h>

#include <core/bus/server.h>
#include <core/bus/tcp_server.h>
#include <core/bus/config.h>

#include <core/rpc/channel.h>
#include <core/rpc/server.h>
#include <core/rpc/redirector_service.h>
#include <core/rpc/throttling_channel.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/meta_state/config.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <core/ytree/ephemeral_node_factory.h>
#include <core/ytree/virtual.h>
#include <core/ytree/yson_file_service.h>
#include <core/ytree/ypath_client.h>

#include <core/profiling/profiling_manager.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <server/misc/build_attributes.h>

#include <server/data_node/config.h>
#include <server/data_node/ytree_integration.h>
#include <server/data_node/chunk_cache.h>
#include <server/data_node/peer_block_table.h>
#include <server/data_node/peer_block_updater.h>
#include <server/data_node/chunk_store.h>
#include <server/data_node/chunk_cache.h>
#include <server/data_node/chunk_registry.h>
#include <server/data_node/block_store.h>
#include <server/data_node/reader_cache.h>
#include <server/data_node/location.h>
#include <server/data_node/data_node_service.h>
#include <server/data_node/master_connector.h>
#include <server/data_node/session_manager.h>
#include <server/data_node/job.h>
#include <server/data_node/private.h>

#include <server/job_agent/job_controller.h>

#include <server/exec_agent/private.h>
#include <server/exec_agent/config.h>
#include <server/exec_agent/slot_manager.h>
#include <server/exec_agent/supervisor_service.h>
#include <server/exec_agent/environment.h>
#include <server/exec_agent/environment_manager.h>
#include <server/exec_agent/unsafe_environment.h>
#include <server/exec_agent/scheduler_connector.h>
#include <server/exec_agent/job.h>

namespace NYT {
namespace NCellNode {

using namespace NBus;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NElection;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler;
using namespace NJobAgent;
using namespace NExecAgent;
using namespace NJobProxy;
using namespace NDataNode;
using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Bootstrap");
static const i64 FootprintMemorySize = (i64) 1024 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellNodeConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
    , MemoryUsageTracker(Config->ExecAgent->JobController->ResourceLimits->Memory, "/cell_node")
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    srand(time(nullptr));

    {
        auto addresses = Config->Addresses;
        if (addresses.find(NNodeTrackerClient::DefaultNetworkName) == addresses.end()) {
            addresses[NNodeTrackerClient::DefaultNetworkName] = TAddressResolver::Get()->GetLocalHostName();
        }
        for (auto& pair : addresses) {
            pair.second = BuildServiceAddress(pair.second, Config->RpcPort);
        }
        LocalDescriptor = NNodeTrackerClient::TNodeDescriptor(addresses);
    }

    LOG_INFO("Starting node (LocalDescriptor: %s, MasterAddresses: [%s])",
        ~ToString(LocalDescriptor),
        ~JoinToString(Config->Masters->Addresses));

    {
        auto result = MemoryUsageTracker.TryAcquire(
            EMemoryConsumer::Footprint,
            FootprintMemorySize);
        if (!result.IsOK()) {
            THROW_ERROR_EXCEPTION("Error allocating footprint memory")
                << result;
        }
    }

    MasterChannel = CreateLeaderChannel(Config->Masters);

    SchedulerChannel = CreateSchedulerChannel(Config->ExecAgent->SchedulerConnector, MasterChannel);

    ControlQueue = New<TActionQueue>("Control");

    BusServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->RpcPort));

    RpcServer = CreateRpcServer(BusServer);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        BIND(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));

    auto jobToMasterChannel = CreateThrottlingChannel(
        Config->JobsToMasterChannel,
        MasterChannel);
    RpcServer->RegisterService(CreateRedirectorService(
        NObjectClient::TObjectServiceProxy::GetServiceName(),
        jobToMasterChannel));
    RpcServer->RegisterService(CreateRedirectorService(
        NChunkClient::TChunkServiceProxy::GetServiceName(),
        jobToMasterChannel));

    ReaderCache = New<TReaderCache>(Config->DataNode);

    ChunkRegistry = New<TChunkRegistry>(this);

    BlockStore = New<TBlockStore>(Config->DataNode, this);
    BlockStore->Initialize();

    PeerBlockTable = New<TPeerBlockTable>(Config->DataNode->PeerBlockTable);

    PeerBlockUpdater = New<TPeerBlockUpdater>(Config->DataNode, this);

    SessionManager = New<TSessionManager>(Config->DataNode, this);

    MasterConnector = New<TMasterConnector>(Config->DataNode, this);

    ChunkStore = New<TChunkStore>(Config->DataNode, this);
    ChunkStore->Initialize();

    ChunkCache = New<TChunkCache>(Config->DataNode, this);
    ChunkCache->Initialize();

    if (!ChunkStore->GetCellGuid().IsEmpty() && !ChunkCache->GetCellGuid().IsEmpty()) {
        if (ChunkStore->GetCellGuid().IsEmpty() != ChunkCache->GetCellGuid().IsEmpty()) {
            THROW_ERROR_EXCEPTION("Inconsistent cell GUID (ChunkStore: %s, ChunkCache: %s)",
                ~ToString(ChunkStore->GetCellGuid()),
                ~ToString(ChunkCache->GetCellGuid()));
        }
        CellGuid = ChunkCache->GetCellGuid();
    }

    if (!ChunkStore->GetCellGuid().IsEmpty() && ChunkCache->GetCellGuid().IsEmpty()) {
        CellGuid = ChunkStore->GetCellGuid();
        ChunkCache->UpdateCellGuid(CellGuid);
    }

    if (ChunkStore->GetCellGuid().IsEmpty() && !ChunkCache->GetCellGuid().IsEmpty()) {
        CellGuid = ChunkCache->GetCellGuid();
        ChunkStore->SetCellGuid(CellGuid);
    }

    ReplicationInThrottler = CreateProfilingThrottlerWrapper(
        CreateLimitedThrottler(Config->DataNode->ReplicationInThrottler),
        DataNodeProfiler.GetPathPrefix() + "/replication_in");
    ReplicationOutThrottler = CreateProfilingThrottlerWrapper(
        CreateLimitedThrottler(Config->DataNode->ReplicationOutThrottler),
        DataNodeProfiler.GetPathPrefix() + "/replication_out");
    RepairInThrottler = CreateProfilingThrottlerWrapper(
        CreateLimitedThrottler(Config->DataNode->RepairInThrottler),
        DataNodeProfiler.GetPathPrefix() + "/repair_in");
    RepairOutThrottler = CreateProfilingThrottlerWrapper(
        CreateLimitedThrottler(Config->DataNode->RepairOutThrottler),
        DataNodeProfiler.GetPathPrefix() + "/repair_out");

    RpcServer->RegisterService(New<TDataNodeService>(Config->DataNode, this));

    JobProxyConfig = New<NJobProxy::TJobProxyConfig>();

    JobProxyConfig->MemoryWatchdogPeriod = Config->ExecAgent->MemoryWatchdogPeriod;

    JobProxyConfig->Logging = Config->ExecAgent->JobProxyLogging;

    JobProxyConfig->MemoryLimitMultiplier = Config->ExecAgent->MemoryLimitMultiplier;

    JobProxyConfig->ForceEnableAccounting = Config->ExecAgent->ForceEnableAccounting;
    JobProxyConfig->EnableCGroupMemoryHierarchy = Config->ExecAgent->EnableCGroupMemoryHierarchy;

    JobProxyConfig->SandboxName = SandboxDirectoryName;
    JobProxyConfig->AddressResolver = Config->AddressResolver;
    JobProxyConfig->SupervisorConnection = New<NBus::TTcpBusClientConfig>();
    JobProxyConfig->SupervisorConnection->Address = LocalDescriptor.GetDefaultAddress();
    JobProxyConfig->SupervisorRpcTimeout = Config->ExecAgent->SupervisorRpcTimeout;
    // TODO(babenko): consider making this priority configurable
    JobProxyConfig->SupervisorConnection->Priority = 6;

    SlotManager = New<TSlotManager>(Config->ExecAgent->SlotManager, this);
    SlotManager->Initialize(Config->ExecAgent->JobController->ResourceLimits->UserSlots);

    JobController = New<TJobController>(Config->ExecAgent->JobController, this);

    auto createExecJob = BIND([this] (
            const NJobAgent::TJobId& jobId,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec) ->
            NJobAgent::IJobPtr
        {
            return NExecAgent::CreateUserJob(
                    jobId,
                    resourceLimits,
                    std::move(jobSpec),
                    this);
        });
    JobController->RegisterFactory(NJobAgent::EJobType::Map,             createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::PartitionMap,    createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SortedMerge,     createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::OrderedMerge,    createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::UnorderedMerge,  createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::Partition,       createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SimpleSort,      createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::PartitionSort,   createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::SortedReduce,    createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::PartitionReduce, createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::ReduceCombiner,  createExecJob);
    JobController->RegisterFactory(NJobAgent::EJobType::RemoteCopy,      createExecJob);

    auto createChunkJob = BIND([this] (
            const NJobAgent::TJobId& jobId,
            const NNodeTrackerClient::NProto::TNodeResources& resourceLimits,
            NJobTrackerClient::NProto::TJobSpec&& jobSpec) ->
            NJobAgent::IJobPtr
        {
            return NDataNode::CreateChunkJob(
                    jobId,
                    std::move(jobSpec),
                    resourceLimits,
                    Config->DataNode,
                    this);
        });
    JobController->RegisterFactory(NJobAgent::EJobType::RemoveChunk,     createChunkJob);
    JobController->RegisterFactory(NJobAgent::EJobType::ReplicateChunk,  createChunkJob);
    JobController->RegisterFactory(NJobAgent::EJobType::RepairChunk,     createChunkJob);

    RpcServer->RegisterService(New<TSupervisorService>(this));

    EnvironmentManager = New<TEnvironmentManager>(Config->ExecAgent->EnvironmentManager);
    EnvironmentManager->Register("unsafe", CreateUnsafeEnvironmentBuilder());

    SchedulerConnector = New<TSchedulerConnector>(Config->ExecAgent->SchedulerConnector, this);

    OrchidRoot = GetEphemeralNodeFactory()->CreateMap();
    SetNodeByYPath(
        OrchidRoot,
        "/monitoring",
        CreateVirtualNode(monitoringManager->GetService()));
    SetNodeByYPath(
        OrchidRoot,
        "/profiling",
        CreateVirtualNode(TProfilingManager::Get()->GetService()));
    SetNodeByYPath(
        OrchidRoot,
        "/config",
        CreateVirtualNode(CreateYsonFileService(ConfigFileName)));
    SetNodeByYPath(
        OrchidRoot,
        "/stored_chunks",
        CreateVirtualNode(CreateStoredChunkMapService(ChunkStore)));
    SetNodeByYPath(
        OrchidRoot,
        "/cached_chunks",
        CreateVirtualNode(CreateCachedChunkMapService(ChunkCache)));

    SetBuildAttributes(OrchidRoot, "node");

    NHttp::TServer httpServer(Config->MonitoringPort);
    httpServer.Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(OrchidRoot->Via(GetControlInvoker())));

    RpcServer->RegisterService(CreateOrchidService(
        OrchidRoot,
        GetControlInvoker()));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    RpcServer->Configure(Config->RpcServer);

    // Do not start subsystems until everything is initialized.
    monitoringManager->Start();
    PeerBlockUpdater->Start();
    MasterConnector->Start();
    SchedulerConnector->Start();
    httpServer.Start();
    RpcServer->Start();

    Sleep(TDuration::Max());
}

TCellNodeConfigPtr TBootstrap::GetConfig() const
{
    return Config;
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue->GetInvoker();
}

IChannelPtr TBootstrap::GetMasterChannel() const
{
    return MasterChannel;
}

IChannelPtr TBootstrap::GetSchedulerChannel() const
{
    return SchedulerChannel;
}

IServerPtr TBootstrap::GetRpcServer() const
{
    return RpcServer;
}

IMapNodePtr TBootstrap::GetOrchidRoot() const
{
    return OrchidRoot;
}

TJobTrackerPtr TBootstrap::GetJobController() const
{
    return JobController;
}

TSlotManagerPtr TBootstrap::GetSlotManager() const
{
    return SlotManager;
}

TEnvironmentManagerPtr TBootstrap::GetEnvironmentManager() const
{
    return EnvironmentManager;
}

TJobProxyConfigPtr TBootstrap::GetJobProxyConfig() const
{
    return JobProxyConfig;
}

TChunkStorePtr TBootstrap::GetChunkStore() const
{
    return ChunkStore;
}

TChunkCachePtr TBootstrap::GetChunkCache() const
{
    return ChunkCache;
}

TNodeMemoryTracker& TBootstrap::GetMemoryUsageTracker()
{
    return MemoryUsageTracker;
}

TChunkRegistryPtr TBootstrap::GetChunkRegistry() const
{
    return ChunkRegistry;
}

TSessionManagerPtr TBootstrap::GetSessionManager() const
{
    return SessionManager;
}

TBlockStorePtr TBootstrap::GetBlockStore() const
{
    return BlockStore;
}

TPeerBlockTablePtr TBootstrap::GetPeerBlockTable() const
{
    return PeerBlockTable;
}

TReaderCachePtr TBootstrap::GetReaderCache() const
{
    return ReaderCache;
}

TMasterConnectorPtr TBootstrap::GetMasterConnector() const
{
    return MasterConnector;
}

const NNodeTrackerClient::TNodeDescriptor& TBootstrap::GetLocalDescriptor() const
{
    return LocalDescriptor;
}

const TGuid& TBootstrap::GetCellGuid() const
{
    return CellGuid;
}

void TBootstrap::UpdateCellGuid(const TGuid& cellGuid)
{
    CellGuid = cellGuid;
    ChunkStore->SetCellGuid(CellGuid);
    ChunkCache->UpdateCellGuid(CellGuid);
}

IThroughputThrottlerPtr TBootstrap::GetReplicationInThrottler() const
{
    return ReplicationInThrottler;
}

IThroughputThrottlerPtr TBootstrap::GetReplicationOutThrottler() const
{
    return ReplicationOutThrottler;
}

IThroughputThrottlerPtr TBootstrap::GetRepairInThrottler() const
{
    return RepairInThrottler;
}

IThroughputThrottlerPtr TBootstrap::GetRepairOutThrottler() const
{
    return RepairOutThrottler;
}

IThroughputThrottlerPtr TBootstrap::GetInThrottler(EWriteSessionType sessionType) const
{
    switch (sessionType) {
        case EWriteSessionType::User:
            return GetUnlimitedThrottler();

        case EWriteSessionType::Repair:
            return RepairInThrottler;

        case EWriteSessionType::Replication:
            return ReplicationInThrottler;

        default:
            YUNREACHABLE();
    }
}

IThroughputThrottlerPtr TBootstrap::GetOutThrottler(EWriteSessionType sessionType) const
{
    switch (sessionType) {
        case EWriteSessionType::User:
            return GetUnlimitedThrottler();

        case EWriteSessionType::Repair:
            return RepairOutThrottler;

        case EWriteSessionType::Replication:
            return ReplicationOutThrottler;

        default:
            YUNREACHABLE();
    }
}

IThroughputThrottlerPtr TBootstrap::GetOutThrottler(EReadSessionType sessionType) const
{
    switch (sessionType) {
        case EReadSessionType::User:
            return GetUnlimitedThrottler();

        case EReadSessionType::Repair:
            return RepairOutThrottler;

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
