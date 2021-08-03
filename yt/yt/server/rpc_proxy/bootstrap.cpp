#include "bootstrap.h"

#include "access_checker.h"
#include "config.h"
#include "dynamic_config_manager.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/server/rpc_proxy/api_service.h>
#include <yt/yt/server/rpc_proxy/discovery_service.h>
#include <yt/yt/server/rpc_proxy/proxy_coordinator.h>
#include <yt/yt/server/rpc_proxy/private.h>

#include <yt/yt/ytlib/program/build_attributes.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>
#include <yt/yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/auth/authentication_manager.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/core_dumper.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/ytalloc/statistics_producer.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/authenticator.h>

#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/grpc/config.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NRpcProxy {

using namespace NAdmin;
using namespace NBus;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;
using namespace NConcurrency;
using namespace NApi;
using namespace NYT::NRpcProxy;
using namespace NAuth;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TProxyConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ControlQueue_(New<TActionQueue>("Control"))
    , WorkerPool_(New<TThreadPool>(Config_->WorkerThreadPoolSize, "Worker"))
    , HttpPoller_(CreateThreadPoolPoller(1, "HttpPoller"))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }

    if (!Config_->ClusterConnection) {
        THROW_ERROR_EXCEPTION("Cluster connection is missing");
    }
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    LocalAddresses_ = NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    YT_LOG_INFO("Starting proxy (LocalAddresses: %v, PrimaryMasterAddresses: %v)",
        GetValues(LocalAddresses_),
        Config_->ClusterConnection->PrimaryMaster->Addresses);

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = GetWorkerInvoker();
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;
    NativeConnection_ = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        std::move(connectionOptions));

    auto clientOptions = TClientOptions::FromUser(NSecurityClient::RootUserName);
    NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

    AuthenticationManager_ = New<TAuthenticationManager>(
        Config_,
        HttpPoller_,
        NativeClient_);
    ProxyCoordinator_ = CreateProxyCoordinator(this);

    DynamicConfigManager_ = CreateDynamicConfigManager(this);
    DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

    AccessChecker_ = CreateAccessChecker(this);

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);
    NProfiling::TSolomonRegistry::Get()->SetDynamicTags({NProfiling::TTag{"proxy_role", DefaultProxyRole}});

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
    SetBuildAttributes(
        orchidRoot,
        "proxy");

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    ApiService_ = CreateApiService(this);
    RpcServer_->RegisterService(ApiService_);

    ProxyCoordinator_->Initialize();
    DynamicConfigManager_->Initialize();
    DynamicConfigManager_->Start();

    if (Config_->DiscoveryService->Enable) {
        DiscoveryService_ = CreateDiscoveryService(this);
        RpcServer_->RegisterService(DiscoveryService_);
    } else {
        ProxyCoordinator_->SetAvailableState(true);
    }

    if (Config_->GrpcServer) {
        GrpcServer_ = NRpc::NGrpc::CreateServer(Config_->GrpcServer);
        GrpcServer_->RegisterService(ApiService_);
        if (Config_->DiscoveryService->Enable) {
            GrpcServer_->RegisterService(DiscoveryService_);
        }
    }

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();

    if (Config_->GrpcServer) {
        const auto& addresses = Config_->GrpcServer->Addresses;
        YT_VERIFY(addresses.size() == 1);

        int port;
        NNet::ParseServiceAddress(addresses[0]->Address, nullptr, &port);

        YT_LOG_INFO("Listening for GRPC requests on port %v", port);
        GrpcServer_->Start();
    }
}

const TProxyConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

TProxyDynamicConfigPtr TBootstrap::GetDynamicConfig() const
{
    return DynamicConfig_.Load();
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetWorkerInvoker() const
{
    return WorkerPool_->GetInvoker();
}

const NNative::IConnectionPtr& TBootstrap::GetNativeConnection() const
{
    return NativeConnection_;
}

const NNative::IClientPtr& TBootstrap::GetNativeClient() const
{
    return NativeClient_;
}

const IAuthenticatorPtr& TBootstrap::GetRpcAuthenticator() const
{
    return AuthenticationManager_->GetRpcAuthenticator();
}

const IProxyCoordinatorPtr& TBootstrap::GetProxyCoordinator() const
{
    return ProxyCoordinator_;
}

const NNodeTrackerClient::TAddressMap& TBootstrap::GetLocalAddresses() const
{
    return LocalAddresses_;
}

const IDynamicConfigManagerPtr& TBootstrap::GetDynamicConfigManager() const
{
    return DynamicConfigManager_;
}

const IAccessCheckerPtr& TBootstrap::GetAccessChecker() const
{
    return AccessChecker_;
}

void TBootstrap::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    ReconfigureSingletons(Config_, newConfig);

    DynamicConfig_.Store(newConfig);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
