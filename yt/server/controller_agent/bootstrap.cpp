#include "bootstrap.h"
#include "config.h"
#include "job_spec_service.h"
#include "controller_agent_service.h"
#include "controller_agent.h"
#include "private.h"

#include <yt/server/scheduler/config.h>

#include <yt/server/admin_server/admin_service.h>

#include <yt/server/misc/address_helpers.h>

#include <yt/ytlib/program/build_attributes.h>
#include <yt/ytlib/program/config.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/core/bus/config.h>
#include <yt/core/bus/server.h>
#include <yt/core/bus/tcp_server.h>

#include <yt/core/http/server.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/net/address.h>
#include <yt/core/net/local_address.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/lfalloc_helpers.h>
#include <yt/core/misc/proc.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/bus_server.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYT {
namespace NControllerAgent {

using namespace NAdmin;
using namespace NBus;
using namespace NElection;
using namespace NHydra;
using namespace NMonitoring;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TControllerAgentBootstrapConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    WarnForUnrecognizedOptions(Logger, Config_);
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    ControlQueue_ = New<TActionQueue>("Control");

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    AgentId_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

    LOG_INFO("Starting controller agent");

    TNativeConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = CreateNativeConnection(Config_->ClusterConnection, connectionOptions);

    TClientOptions clientOptions;
    clientOptions.User = NSecurityClient::SchedulerUserName;
    Client_ = Connection_->CreateNativeClient(clientOptions);

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = CreateBusServer(BusServer_);

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    NodeDirectory_ = New<TNodeDirectory>();

    NodeDirectorySynchronizer_ = New<TNodeDirectorySynchronizer>(
        Config_->NodeDirectorySynchronizer,
        Connection_,
        NodeDirectory_);
    NodeDirectorySynchronizer_->Start();

    ControllerAgent_ = New<TControllerAgent>(Config_->ControllerAgent, this);

    if (Config_->CoreDumper) {
        CoreDumper_ = New<TCoreDumper>(Config_->CoreDumper);
    }

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        TRefCountedTracker::Get()->GetMonitoringProducer());
    MonitoringManager_->Start();

    LFAllocProfiler_ = std::make_unique<NLFAlloc::TLFAllocProfiler>();

    ControllerAgent_->Initialize();

    auto orchidRoot = NYTree::GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/controller_agent",
        CreateVirtualNode(ControllerAgent_->GetOrchidService()));

    SetBuildAttributes(orchidRoot, "controller_agent");

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        CoreDumper_));

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    HttpServer_->AddHandler(
        "/orchid/",
        NMonitoring::GetOrchidYPathHttpHandler(orchidRoot->Via(GetControlInvoker())));

    RpcServer_->RegisterService(CreateJobSpecService(this));
    RpcServer_->RegisterService(CreateControllerAgentService(this));

    LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();
}

const TAgentId& TBootstrap::GetAgentId() const
{
    return AgentId_;
}

const TControllerAgentBootstrapConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const INativeClientPtr& TBootstrap::GetMasterClient() const
{
    return Client_;
}

TAddressMap TBootstrap::GetLocalAddresses() const
{
    return NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);
}

TNetworkPreferenceList TBootstrap::GetLocalNetworks() const
{
    return Config_->Addresses.empty()
        ? DefaultNetworkPreferences
        : GetIths<0>(Config_->Addresses);
}

IInvokerPtr TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const TControllerAgentPtr& TBootstrap::GetControllerAgent() const
{
    return ControllerAgent_;
}

const TNodeDirectoryPtr& TBootstrap::GetNodeDirectory() const
{
    return NodeDirectory_;
}

const TCoreDumperPtr& TBootstrap::GetCoreDumper() const
{
    return CoreDumper_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT
