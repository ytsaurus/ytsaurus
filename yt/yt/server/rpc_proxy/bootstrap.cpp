#include "bootstrap.h"

#include "access_checker.h"
#include "config.h"
#include "discovery_service.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/library/auth_server/authentication_manager.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/library/coredumper/coredumper.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

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
    , WorkerPool_(CreateThreadPool(Config_->WorkerThreadPoolSize, "Worker"))
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

    YT_LOG_INFO("Starting proxy (LocalAddresses: %v)",
        GetValues(LocalAddresses_));

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = GetWorkerInvoker();
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;
    NativeConnection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection, std::move(connectionOptions));

    NativeConnection_->GetClusterDirectorySynchronizer()->Start();
    NativeConnection_->GetNodeDirectorySynchronizer()->Start();
    NativeConnection_->GetQueueConsumerRegistrationManager()->StartSync();

    NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

    auto clientOptions = TClientOptions::FromUser(NSecurityClient::RootUserName);
    NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

    AuthenticationManager_ = CreateAuthenticationManager(
        Config_,
        HttpPoller_,
        NativeClient_);

    if (Config_->TvmOnlyRpcPort && Config_->TvmOnlyAuth) {
        TvmOnlyAuthenticationManager_ = CreateAuthenticationManager(
            Config_->TvmOnlyAuth,
            HttpPoller_,
            NativeClient_);
    }

    ProxyCoordinator_ = CreateProxyCoordinator();
    TraceSampler_ = New<NTracing::TSampler>();

    DynamicConfigManager_ = CreateDynamicConfigManager(this);
    DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

    AccessChecker_ = CreateAccessChecker(this);

    BusServer_ = CreateBusServer(Config_->BusServer);
    if (Config_->TvmOnlyRpcPort) {
        auto busConfigCopy = CloneYsonSerializable(Config_->BusServer);
        busConfigCopy->Port = Config_->TvmOnlyRpcPort;
        TvmOnlyBusServer_ = CreateBusServer(busConfigCopy);
    }

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
    if (TvmOnlyBusServer_) {
        TvmOnlyRpcServer_ = NRpc::NBus::CreateBusServer(TvmOnlyBusServer_);
    }

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
        CreateVirtualNode(ConfigNode_));
    SetNodeByYPath(
        orchidRoot,
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/cluster_connection",
        CreateVirtualNode(NativeConnection_->GetOrchidService()));
    SetBuildAttributes(
        orchidRoot,
        "proxy");

    auto orchidService = CreateOrchidService(
        orchidRoot,
        GetControlInvoker(),
        NativeAuthenticator_);
    RpcServer_->RegisterService(orchidService);
    if (TvmOnlyRpcServer_) {
        TvmOnlyRpcServer_->RegisterService(orchidService);
    }

    ApiService_ = CreateApiService(
        this,
        AuthenticationManager_->GetRpcAuthenticator(),
        RpcProxyLogger,
        Config_->ApiService,
        RpcProxyProfiler);

    if (TvmOnlyAuthenticationManager_) {
        TvmOnlyApiService_ = CreateApiService(
            this,
            TvmOnlyAuthenticationManager_->GetRpcAuthenticator(),
            RpcProxyLogger,
            Config_->ApiService,
            RpcProxyProfiler);
    }

    RpcServer_->RegisterService(ApiService_);
    if (TvmOnlyRpcServer_ && TvmOnlyApiService_) {
        TvmOnlyRpcServer_->RegisterService(TvmOnlyApiService_);
    }

    DynamicConfigManager_->Initialize();
    DynamicConfigManager_->Start();

    // NB: We must apply the first dynamic config before ApiService_ starts.
    YT_LOG_INFO("Waiting for dynamic config");
    WaitFor(DynamicConfigManager_->GetConfigLoadedFuture())
        .ThrowOnError();

    if (Config_->DiscoveryService->Enable) {
        DiscoveryService_ = CreateDiscoveryService(this);
        RpcServer_->RegisterService(DiscoveryService_);
        if (TvmOnlyRpcServer_) {
            TvmOnlyRpcServer_->RegisterService(DiscoveryService_);
        }
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

    auto adminService = CreateAdminService(
        GetControlInvoker(),
        /*coreDumper*/ nullptr,
        NativeAuthenticator_);
    RpcServer_->RegisterService(adminService);
    if (TvmOnlyRpcServer_) {
        TvmOnlyRpcServer_->RegisterService(adminService);
    }

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();

    if (TvmOnlyRpcServer_) {
        YT_LOG_INFO("Listening for TVM-only RPC requests on port %v", Config_->TvmOnlyRpcPort);
        auto rpcServerConfigCopy = CloneYsonSerializable(Config_->RpcServer);
        TvmOnlyRpcServer_->Configure(rpcServerConfigCopy);
        TvmOnlyRpcServer_->Start();
    }

    if (Config_->GrpcServer) {
        const auto& addresses = Config_->GrpcServer->Addresses;
        YT_VERIFY(addresses.size() == 1);

        int port;
        NNet::ParseServiceAddress(addresses[0]->Address, nullptr, &port);

        YT_LOG_INFO("Listening for GRPC requests on port %v", port);
        GrpcServer_->Start();
    }
}

////////////////////////////////////////////////////////////////////////////////

const IInvokerPtr& TBootstrap::GetWorkerInvoker() const
{
    return WorkerPool_->GetInvoker();
}

const IAuthenticatorPtr& TBootstrap::GetRpcAuthenticator() const
{
    return AuthenticationManager_->GetRpcAuthenticator();
}

TAuthenticationManagerConfigPtr TBootstrap::GetConfigAuthenticationManager() const
{
    return Config_;
}

const NTracing::TSamplerPtr& TBootstrap::GetTraceSampler() const
{
    return TraceSampler_;
}

const IProxyCoordinatorPtr& TBootstrap::GetProxyCoordinator() const
{
    return ProxyCoordinator_;
}

const IAccessCheckerPtr& TBootstrap::GetAccessChecker() const
{
    return AccessChecker_;
}

const NNative::IConnectionPtr& TBootstrap::GetNativeConnection() const
{
    return NativeConnection_;
}

const NNative::IClientPtr& TBootstrap::GetNativeClient() const
{
    return NativeClient_;
}

////////////////////////////////////////////////////////////////////////////////

const TProxyConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const NNodeTrackerClient::TAddressMap& TBootstrap::GetLocalAddresses() const
{
    return LocalAddresses_;
}

const IDynamicConfigManagerPtr& TBootstrap::GetDynamicConfigManager() const
{
    return DynamicConfigManager_;
}

////////////////////////////////////////////////////////////////////////////////

void TBootstrap::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    ReconfigureNativeSingletons(Config_, newConfig);

    TraceSampler_->UpdateConfig(newConfig->Tracing);

    NativeConnection_->Reconfigure(newConfig->ClusterConnection);

    ApiService_->OnDynamicConfigChanged(newConfig->Api);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
