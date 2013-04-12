#include "stdafx.h"
#include "bootstrap.h"
#include "config.h"

#include <ytlib/misc/address.h>
#include <ytlib/misc/ref_counted_tracker.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/server.h>
#include <ytlib/bus/tcp_server.h>
#include <ytlib/bus/config.h>

#include <ytlib/rpc/channel.h>
#include <ytlib/rpc/server.h>
#include <ytlib/rpc/redirector_service.h>

#include <ytlib/meta_state/master_channel.h>

#include <ytlib/meta_state/config.h>

#include <ytlib/orchid/orchid_service.h>

#include <ytlib/monitoring/monitoring_manager.h>
#include <ytlib/monitoring/ytree_integration.h>
#include <ytlib/monitoring/http_server.h>
#include <ytlib/monitoring/http_integration.h>

#include <ytlib/ytree/ephemeral_node_factory.h>
#include <ytlib/ytree/virtual.h>
#include <ytlib/ytree/yson_file_service.h>
#include <ytlib/ytree/ypath_client.h>

#include <ytlib/profiling/profiling_manager.h>

#include <ytlib/scheduler/scheduler_channel.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <server/chunk_holder/private.h>
#include <server/chunk_holder/config.h>
#include <server/chunk_holder/ytree_integration.h>
#include <server/chunk_holder/chunk_cache.h>
#include <server/chunk_holder/peer_block_table.h>
#include <server/chunk_holder/peer_block_updater.h>
#include <server/chunk_holder/chunk_store.h>
#include <server/chunk_holder/chunk_cache.h>
#include <server/chunk_holder/chunk_registry.h>
#include <server/chunk_holder/block_store.h>
#include <server/chunk_holder/reader_cache.h>
#include <server/chunk_holder/location.h>
#include <server/chunk_holder/data_node_service.h>
#include <server/chunk_holder/master_connector.h>
#include <server/chunk_holder/session_manager.h>
#include <server/chunk_holder/job_executor.h>

#include <server/exec_agent/private.h>
#include <server/exec_agent/config.h>
#include <server/exec_agent/job_manager.h>
#include <server/exec_agent/supervisor_service.h>
#include <server/exec_agent/environment.h>
#include <server/exec_agent/environment_manager.h>
#include <server/exec_agent/unsafe_environment.h>
#include <server/exec_agent/scheduler_connector.h>

#include <server/misc/build_attributes.h>

namespace NYT {
namespace NCellNode {

using namespace NBus;
using namespace NChunkServer;
using namespace NElection;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NScheduler;
using namespace NExecAgent;
using namespace NJobProxy;
using namespace NChunkHolder;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger SILENT_UNUSED Logger("Bootstrap");

static const i64 FootprintMemorySize = (i64) 1024 * 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    const Stroka& configFileName,
    TCellNodeConfigPtr config)
    : ConfigFileName(configFileName)
    , Config(config)
    , MemoryUsageTracker(Config->TotalMemorySize, "/cell_node")
{ }

TBootstrap::~TBootstrap()
{ }

void TBootstrap::Run()
{
    srand(time(NULL));

    LocalDescriptor.Address = BuildServiceAddress(
        TAddressResolver::Get()->GetLocalHostName(),
        Config->RpcPort);

    LOG_INFO("Starting node (LocalDescriptor: %s, MasterAddresses: [%s])",
        ~ToString(LocalDescriptor),
        ~JoinToString(Config->Masters->Addresses));

    auto result = MemoryUsageTracker.TryAcquire(
        EMemoryConsumer::Footprint,
        FootprintMemorySize);
    if (!result.IsOK()) {
        auto error = TError("Error allocating footprint memory") << result;
        // TODO(psushin): no need to create core here.
        LOG_FATAL(error);
    }

    MasterChannel = CreateLeaderChannel(Config->Masters);

    SchedulerChannel = CreateSchedulerChannel(
        Config->ExecAgent->SchedulerConnector->RpcTimeout,
        MasterChannel);

    ControlQueue = New<TActionQueue>("Control");

    BusServer = CreateTcpBusServer(New<TTcpBusServerConfig>(Config->RpcPort));

    RpcServer = CreateRpcServer(BusServer);

    auto monitoringManager = New<TMonitoringManager>();
    monitoringManager->Register(
        "/ref_counted",
        BIND(&TRefCountedTracker::GetMonitoringInfo, TRefCountedTracker::Get()));
    monitoringManager->Start();

    auto masterObjectServiceRedirector = CreateRedirectorService(
        NObjectClient::TObjectServiceProxy::GetServiceName(),
        MasterChannel);
    RpcServer->RegisterService(masterObjectServiceRedirector);

    ReaderCache = New<TReaderCache>(Config->DataNode);

    ChunkRegistry = New<TChunkRegistry>(this);

    BlockStore = New<TBlockStore>(Config->DataNode, this);

    PeerBlockTable = New<TPeerBlockTable>(Config->DataNode->PeerBlockTable);

    PeerBlockUpdater = New<TPeerBlockUpdater>(Config->DataNode, this);
    PeerBlockUpdater->Start();

    ChunkStore = New<TChunkStore>(Config->DataNode, this);
    ChunkStore->Start();

    ChunkCache = New<TChunkCache>(Config->DataNode, this);
    ChunkCache->Start();

    if (!ChunkStore->GetCellGuid().IsEmpty() && !ChunkCache->GetCellGuid().IsEmpty()) {
        LOG_FATAL_IF(
            ChunkStore->GetCellGuid().IsEmpty() != ChunkCache->GetCellGuid().IsEmpty(),
            "Inconsistent cell guid (ChunkStore: %s, ChunkCache: %s)",
            ~ToString(ChunkStore->GetCellGuid()),
            ~ToString(ChunkCache->GetCellGuid()));
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

    SessionManager = New<TSessionManager>(Config->DataNode, this);

    JobExecutor = New<TJobExecutor>(this);

    MasterConnector = New<TMasterConnector>(Config->DataNode, this);

    auto dataNodeService = New<TDataNodeService>(Config->DataNode, this);
    RpcServer->RegisterService(dataNodeService);

    MasterConnector->Start();

    JobProxyConfig = New<NJobProxy::TJobProxyConfig>();

    JobProxyConfig->MemoryWatchdogPeriod = Config->ExecAgent->MemoryWatchdogPeriod;

    JobProxyConfig->Logging = Config->ExecAgent->JobProxyLogging;

    JobProxyConfig->MemoryLimitMultiplier = Config->ExecAgent->MemoryLimitMultiplier;

    JobProxyConfig->SandboxName = SandboxDirectoryName;
    JobProxyConfig->AddressResolver = Config->AddressResolver;
    JobProxyConfig->SupervisorConnection = New<NBus::TTcpBusClientConfig>();
    JobProxyConfig->SupervisorConnection->Address = LocalDescriptor.Address;
    JobProxyConfig->SupervisorRpcTimeout = Config->ExecAgent->SupervisorRpcTimeout;
    JobProxyConfig->MasterRpcTimeout = Config->Masters->RpcTimeout;
    // TODO(babenko): consider making this priority configurable
    JobProxyConfig->SupervisorConnection->Priority = 6;

    JobControlEnabled = false;

#if defined(_unix_) && !defined(_darwin_)
    if (Config->EnforceJobControl) {
        uid_t ruid, euid, suid;
        YCHECK(getresuid(&ruid, &euid, &suid) == 0);
        if (suid == 0) {
            JobControlEnabled = true;
        }
        umask(0000);
    }
#endif

    if (!JobControlEnabled) {
        if (Config->ExecAgent->EnforceJobControl) {
            LOG_FATAL("Job control disabled, please run as root");
        } else {
            LOG_WARNING("Job control disabled, cannot kill jobs and use memory limits watcher");
        }
    }

    JobManager = New<TJobManager>(Config->ExecAgent->JobManager, this);
    JobManager->Initialize();

    auto supervisorService = New<TSupervisorService>(this);
    RpcServer->RegisterService(supervisorService);

    EnvironmentManager = New<TEnvironmentManager>(Config->ExecAgent->EnvironmentManager);
    EnvironmentManager->Register("unsafe", CreateUnsafeEnvironmentBuilder());

    SchedulerConnector = New<TSchedulerConnector>(Config->ExecAgent->SchedulerConnector, this);
    SchedulerConnector->Start();

    OrchidRoot = GetEphemeralNodeFactory()->CreateMap();
    SetNodeByYPath(
        OrchidRoot,
        "/monitoring",
        CreateVirtualNode(CreateMonitoringProducer(monitoringManager)));
    SetNodeByYPath(
        OrchidRoot,
        "/profiling",
        CreateVirtualNode(
        TProfilingManager::Get()->GetRoot()
        ->Via(TProfilingManager::Get()->GetInvoker())));
    SetNodeByYPath(
        OrchidRoot,
        "/config",
        CreateVirtualNode(CreateYsonFileProducer(ConfigFileName)));
    SetNodeByYPath(
        OrchidRoot,
        "/stored_chunks",
        CreateVirtualNode(CreateStoredChunkMapService(~ChunkStore)));
    SetNodeByYPath(
        OrchidRoot,
        "/cached_chunks",
        CreateVirtualNode(CreateCachedChunkMapService(~ChunkCache)));
    SyncYPathSet(
        OrchidRoot,
        "/@service_name", ConvertToYsonString("node"));
    SetBuildAttributes(OrchidRoot);

    ::THolder<NHttp::TServer> httpServer(new NHttp::TServer(Config->MonitoringPort));
    httpServer->Register(
        "/orchid",
        NMonitoring::GetYPathHttpHandler(OrchidRoot->Via(GetControlInvoker())));

    auto orchidService = New<TOrchidService>(
        OrchidRoot,
        GetControlInvoker());
    RpcServer->RegisterService(orchidService);

    LOG_INFO("Listening for HTTP requests on port %d", Config->MonitoringPort);
    httpServer->Start();

    LOG_INFO("Listening for RPC requests on port %d", Config->RpcPort);
    RpcServer->Configure(Config->RpcServer);
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

TJobManagerPtr TBootstrap::GetJobManager() const
{
    return JobManager;
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

TJobExecutorPtr TBootstrap::GetJobExecutor() const
{
    return JobExecutor;
}

TBlockStorePtr TBootstrap::GetBlockStore()
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

bool TBootstrap::IsJobControlEnabled() const
{
    return JobControlEnabled;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellNode
} // namespace NYT
