#include "bootstrap.h"

#include "config.h"
#include "job_prober_service.h"
#include "controller_agent_service.h"
#include "controller_agent.h"
#include "job_tracker_service.h"
#include "private.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NControllerAgent {

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
using namespace NCoreDump;
using namespace NFusion;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TControllerAgentBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ServiceLocator_(std::move(serviceLocator))
    , ControlQueue_(New<TActionQueue>("Control"))
    , ConnectionThreadPool_(CreateThreadPool(
        Config_->ClusterConnection->Dynamic->ThreadPoolSize,
        "Connection"))
    , AgentId_(NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), Config_);
    } else {
        WarnForUnrecognizedOptions(Logger(), Config_);
    }
}

TBootstrap::~TBootstrap() = default;

TFuture<void> TBootstrap::Run()
{
    return BIND(&TBootstrap::DoRun, MakeStrong(this))
        .AsyncVia(GetControlInvoker())
        .Run();
}

void TBootstrap::DoRun()
{
    DoInitialize();
    DoStart();
}

void TBootstrap::DoInitialize()
{
    YT_LOG_INFO("Starting controller agent");

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = GetConnectionInvoker();
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection, std::move(connectionOptions));
    Connection_->InitializeDiscoveryServerAddressPool();

    NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

    // Force start node directory synchronizer.
    Connection_->GetNodeDirectorySynchronizer()->Start();

    Connection_->GetClusterDirectorySynchronizer()->Start();

    Connection_->GetMasterCellDirectorySynchronizer()->Start();

    auto clientOptions = TClientOptions::FromUser(NSecurityClient::SchedulerUserName);
    Client_ = Connection_->CreateNativeClient(clientOptions);

    NLogging::GetDynamicTableLogWriterFactory()->SetClient(Client_);

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    ControllerAgent_ = New<TControllerAgent>(Config_->ControllerAgent, ConfigNode_->AsMap()->FindChild("controller_agent"), this);

    CoreDumper_ = ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>();

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
        &MonitoringManager_,
        &orchidRoot);

    ControllerAgent_->Initialize();

    if (Config_->ExposeConfigInOrchid) {
        SetNodeByYPath(
            orchidRoot,
            "/config",
            CreateVirtualNode(ConfigNode_));
    }
    SetNodeByYPath(
        orchidRoot,
        "/controller_agent",
        CreateVirtualNode(ControllerAgent_->CreateOrchidService()->Via(GetControlInvoker())));
    if (auto coreDumper = GetCoreDumper()) {
        SetNodeByYPath(
            orchidRoot,
            "/core_dumper",
            CreateVirtualNode(coreDumper->CreateOrchidService()));
    }
    SetBuildAttributes(
        orchidRoot,
        "controller_agent");

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        GetCoreDumper(),
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(),
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateControllerAgentService(this));
    RpcServer_->RegisterService(CreateJobProberService(this));
    RpcServer_->RegisterService(CreateJobTrackerService(this));
}

void TBootstrap::DoStart()
{
    YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
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

const NNative::IClientPtr& TBootstrap::GetClient() const
{
    return Client_;
}

TAddressMap TBootstrap::GetLocalAddresses() const
{
    return NServer::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);
}

TNetworkPreferenceList TBootstrap::GetLocalNetworks() const
{
    return Config_->Addresses.empty()
        ? DefaultNetworkPreferences
        : GetIths<0>(Config_->Addresses);
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetConnectionInvoker() const
{
    return ConnectionThreadPool_->GetInvoker();
}

const TControllerAgentPtr& TBootstrap::GetControllerAgent() const
{
    return ControllerAgent_;
}

const TNodeDirectoryPtr& TBootstrap::GetNodeDirectory() const
{
    return Connection_->GetNodeDirectory();
}

const ICoreDumperPtr& TBootstrap::GetCoreDumper() const
{
    return CoreDumper_;
}

const IAuthenticatorPtr& TBootstrap::GetNativeAuthenticator() const
{
    return NativeAuthenticator_;
}

void TBootstrap::OnDynamicConfigChanged(const TControllerAgentConfigPtr& config)
{
    TSingletonManager::Reconfigure(config);

    RpcServer_->OnDynamicConfigChanged(config->RpcServer);
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateControllerAgentBootstrap(
    TControllerAgentBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
