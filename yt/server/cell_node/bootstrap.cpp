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
#include <core/rpc/bus_channel.h>
#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/server.h>
#include <core/rpc/bus_server.h>
#include <core/rpc/redirector_service.h>
#include <core/rpc/throttling_channel.h>

#include <ytlib/hydra/peer_channel.h>

#include <ytlib/hydra/config.h>

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

#include <ytlib/hive/cell_directory.h>
#include <ytlib/hive/timestamp_provider.h>
#include <ytlib/hive/remote_timestamp_provider.h>

#include <ytlib/transaction_client/transaction_manager.h>

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

#include <server/tablet_node/tablet_cell_controller.h>
#include <server/tablet_node/store_flusher.h>
#include <server/tablet_node/store_compactor.h>

#include <server/query_agent/query_executor.h>
#include <server/query_agent/query_service.h>

namespace NYT {
namespace NCellNode {

using namespace NBus;
using namespace NChunkClient;
using namespace NChunkServer;
using namespace NElection;
using namespace NHydra;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NScheduler;
using namespace NJobAgent;
using namespace NExecAgent;
using namespace NJobProxy;
using namespace NDataNode;
using namespace NTabletNode;
using namespace NHive;
using namespace NQueryAgent;

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
        auto localHostName = TAddressResolver::Get()->GetLocalHostName();
        LocalDescriptor.Address = BuildServiceAddress(localHostName, Config->RpcPort);
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

    MasterChannel = CreatePeerChannel(
        Config->Masters,
        GetBusChannelFactory(),
        EPeerRole::Leader);

    SchedulerChannel = CreateSchedulerChannel(
        Config->ExecAgent->SchedulerConnector,
        GetBusChannelFactory(),
        MasterChannel);

    ControlQueue = New<TActionQueue>("Control");

    QueryWorkerPool = New<TThreadPool>(
        Config->QueryAgent->ThreadPoolSize,
        "Query");

    BusServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->RpcPort));

    RpcServer = CreateBusServer(BusServer);

    TabletChannelFactory = CreateCachingChannelFactory(GetBusChannelFactory());

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        TRefCountedTracker::Get()->GetMonitoringProducer());

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

    PeerBlockTable = New<TPeerBlockTable>(Config->DataNode->PeerBlockTable);

    PeerBlockUpdater = New<TPeerBlockUpdater>(Config->DataNode, this);

    SessionManager = New<TSessionManager>(Config->DataNode, this);

    MasterConnector = New<NDataNode::TMasterConnector>(Config->DataNode, this);

    ChunkStore = New<NDataNode::TChunkStore>(Config->DataNode, this);

    ChunkCache = New<TChunkCache>(Config->DataNode, this);

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

    JobProxyConfig->SandboxName = SandboxDirectoryName;
    JobProxyConfig->AddressResolver = Config->AddressResolver;
    JobProxyConfig->SupervisorConnection = New<NBus::TTcpBusClientConfig>();
    JobProxyConfig->SupervisorConnection->Address = LocalDescriptor.Address;
    JobProxyConfig->SupervisorRpcTimeout = Config->ExecAgent->SupervisorRpcTimeout;
    // TODO(babenko): consider making this priority configurable
    JobProxyConfig->SupervisorConnection->Priority = 6;

    SlotManager = New<TSlotManager>(Config->ExecAgent->SlotManager, this);

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
    JobController->RegisterFactory(NJobAgent::EJobType::ReduceCombiner, createExecJob);

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

    CellDirectory = New<TCellDirectory>(
        Config->CellDirectory,
        GetBusChannelFactory());
    CellDirectory->RegisterCell(Config->Masters);

    TimestampProvider = CreateRemoteTimestampProvider(
        Config->TimestampProvider,
        GetBusChannelFactory());

    TransactionManager = New<NTransactionClient::TTransactionManager>(
        Config->TransactionManager,
        Config->Masters->CellGuid,
        MasterChannel,
        TimestampProvider,
        CellDirectory);

    TabletCellController = New<TTabletCellController>(Config, this);

    auto queryExecutor = CreateQueryExecutor(this);

    auto storeFlusher = New<TStoreFlusher>(Config->TabletNode->StoreFlusher, this);

    auto storeCompactor = New<TStoreCompactor>(Config->TabletNode->StoreCompactor, this);

    RpcServer->RegisterService(CreateQueryService(
        GetControlInvoker(),
        queryExecutor));

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
    SetNodeByYPath(
        OrchidRoot,
        "/tablet_slots",
        CreateVirtualNode(TabletCellController->GetOrchidService()
            ->Via(GetControlInvoker())));
    SetBuildAttributes(OrchidRoot, "node");

    NHttp::TServer httpServer(Config->MonitoringPort);
    httpServer.Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(OrchidRoot->Via(GetControlInvoker())));

    RpcServer->RegisterService(New<TOrchidService>(
        OrchidRoot,
        GetControlInvoker()));

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    RpcServer->Configure(Config->RpcServer);

    // Do not start subsystems until everything is initialized.
    TabletCellController->Initialize();
    BlockStore->Initialize();
    ChunkStore->Initialize();
    ChunkCache->Initialize();
    SlotManager->Initialize(Config->ExecAgent->JobController->ResourceLimits->UserSlots);
    monitoringManager->Start();
    PeerBlockUpdater->Start();
    MasterConnector->Start();
    SchedulerConnector->Start();
    storeFlusher->Start();
    storeCompactor->Start();
    RpcServer->Start();
    httpServer.Start();

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

IInvokerPtr TBootstrap::GetQueryWorkerInvoker() const
{
    return QueryWorkerPool->GetInvoker();
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

IChannelFactoryPtr TBootstrap::GetTabletChannelFactory() const
{
    return TabletChannelFactory;
}

IMapNodePtr TBootstrap::GetOrchidRoot() const
{
    return OrchidRoot;
}

TJobTrackerPtr TBootstrap::GetJobController() const
{
    return JobController;
}

TTabletCellControllerPtr TBootstrap::GetTabletCellController() const
{
    return TabletCellController;
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

NDataNode::TChunkStorePtr TBootstrap::GetChunkStore() const
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

NDataNode::TMasterConnectorPtr TBootstrap::GetMasterConnector() const
{
    return MasterConnector;
}

TCellDirectoryPtr TBootstrap::GetCellDirectory() const
{
    return CellDirectory;
}

ITimestampProviderPtr TBootstrap::GetTimestampProvider() const
{
    return TimestampProvider;
}

NTransactionClient::TTransactionManagerPtr TBootstrap::GetTransactionManager() const
{
    return TransactionManager;
}

const NNodeTrackerClient::TNodeDescriptor& TBootstrap::GetLocalDescriptor() const
{
    return LocalDescriptor;
}

const TGuid& TBootstrap::GetCellGuid() const
{
    return Config->Masters->CellGuid;
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
