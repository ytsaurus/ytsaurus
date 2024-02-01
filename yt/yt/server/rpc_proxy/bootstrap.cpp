#include "bootstrap.h"

#include "access_checker.h"
#include "config.h"
#include "discovery_service.h"
#include "dynamic_config_manager.h"
#include "private.h"

#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/profilers.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>
#include <yt/yt/server/lib/rpc_proxy/security_manager.h>

#include <yt/yt/server/lib/admin/admin_service.h>
#include <yt/yt/server/lib/admin/restart_service.h>

#include <yt/yt/server/lib/misc/address_helpers.h>
#include <yt/yt/server/lib/misc/restart_manager.h>
#include <yt/yt/server/lib/misc/disk_change_checker.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/library/containers/disk_manager/config.h>
#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>
#include <yt/yt/library/containers/disk_manager/disk_manager_proxy.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/misc/memory_reference_tracker.h>
#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

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

    MemoryUsageTracker_ = CreateNodeMemoryTracker(
        *Config_->MemoryLimits->Total,
        /*limits*/ {},
        Logger,
        RpcProxyProfiler.WithPrefix("/memory_usage"));

    ReconfigureMemoryLimits(Config_->MemoryLimits);

    MemoryReferenceTracker_ = CreateNodeMemoryReferenceTracker(MemoryUsageTracker_);

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = GetWorkerInvoker();
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;
    Connection_ = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        std::move(connectionOptions),
        {},
        MemoryUsageTracker_);

    Connection_->GetClusterDirectorySynchronizer()->Start();
    Connection_->GetNodeDirectorySynchronizer()->Start();
    Connection_->GetQueueConsumerRegistrationManager()->StartSync();

    NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

    RootClient_ = Connection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

    AuthenticationManager_ = CreateAuthenticationManager(
        Config_,
        HttpPoller_,
        RootClient_);

    if (Config_->TvmOnlyRpcPort && Config_->TvmOnlyAuth) {
        TvmOnlyAuthenticationManager_ = CreateAuthenticationManager(
            Config_->TvmOnlyAuth,
            HttpPoller_,
            RootClient_);
    }

    ProxyCoordinator_ = CreateProxyCoordinator();
    TraceSampler_ = New<NTracing::TSampler>();

    DynamicConfigManager_ = CreateDynamicConfigManager(
        Config_,
        ProxyCoordinator_,
        Connection_,
        GetControlInvoker());
    AccessChecker_ = CreateAccessChecker(
        Config_->AccessChecker,
        ProxyCoordinator_,
        Connection_,
        DynamicConfigManager_);

    BusServer_ = CreateBusServer(Config_->BusServer);
    if (Config_->TvmOnlyRpcPort) {
        auto busConfigCopy = CloneYsonStruct(Config_->BusServer);
        busConfigCopy->Port = Config_->TvmOnlyRpcPort;
        TvmOnlyBusServer_ = CreateBusServer(busConfigCopy);
    }

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
    RpcServer_->Configure(Config_->RpcServer);

    if (TvmOnlyBusServer_) {
        TvmOnlyRpcServer_ = NRpc::NBus::CreateBusServer(TvmOnlyBusServer_);
    }

    DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    DiskManagerProxy_ = CreateDiskManagerProxy(Config_->DiskManagerProxy);
    DiskInfoProvider_ = New<NContainers::TDiskInfoProvider>(
        DiskManagerProxy_,
        Config_->DiskInfoProvider);
    DiskChangeChecker_ = New<TDiskChangeChecker>(
        DiskInfoProvider_,
        GetControlInvoker(),
        Logger);

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);
    NProfiling::TSolomonRegistry::Get()->SetDynamicTags({NProfiling::TTag{"proxy_role", DefaultRpcProxyRole}});

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
        CreateVirtualNode(Connection_->GetOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/disk_monitoring",
        CreateVirtualNode(DiskChangeChecker_->GetOrchidService()));
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

    auto securityManager = CreateSecurityManager(
        Config_->ApiService->SecurityManager,
        Connection_,
        Logger);

    auto createApiService = [&] (const NAuth::IAuthenticationManagerPtr& authenticationManager) {
        return CreateApiService(
            Config_->ApiService,
            GetControlInvoker(),
            GetWorkerInvoker(),
            Connection_,
            authenticationManager->GetRpcAuthenticator(),
            ProxyCoordinator_,
            AccessChecker_,
            securityManager,
            TraceSampler_,
            RpcProxyLogger,
            RpcProxyProfiler,
            MemoryUsageTracker_,
            MemoryReferenceTracker_);
    };

    ApiService_ = createApiService(AuthenticationManager_);
    if (TvmOnlyAuthenticationManager_) {
        TvmOnlyApiService_ = createApiService(TvmOnlyAuthenticationManager_);
    }

    RpcServer_->RegisterService(ApiService_);
    if (TvmOnlyRpcServer_ && TvmOnlyApiService_) {
        TvmOnlyRpcServer_->RegisterService(TvmOnlyApiService_);
    }

    DiskChangeChecker_->Start();
    DynamicConfigManager_->Initialize();
    DynamicConfigManager_->Start();

    // NB: We must apply the first dynamic config before ApiService_ starts.
    YT_LOG_INFO("Waiting for dynamic config");
    WaitFor(DynamicConfigManager_->GetConfigLoadedFuture())
        .ThrowOnError();

    if (Config_->DiscoveryService->Enable) {
        DiscoveryService_ = CreateDiscoveryService(
            Config_,
            ProxyCoordinator_,
            Connection_,
            GetControlInvoker(),
            GetWorkerInvoker(),
            LocalAddresses_);
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

    auto restartManager = New<TRestartManager>(GetControlInvoker());
    RpcServer_->RegisterService(CreateRestartService(
        restartManager,
        GetControlInvoker(),
        RpcProxyLogger,
        NativeAuthenticator_));

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Start();

    if (TvmOnlyRpcServer_) {
        YT_LOG_INFO("Listening for TVM-only RPC requests on port %v", Config_->TvmOnlyRpcPort);
        auto rpcServerConfigCopy = CloneYsonStruct(Config_->RpcServer);
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

    SetNodeByYPath(
        orchidRoot,
        "/rpc_proxy",
        CreateVirtualNode(ApiService_->CreateOrchidService()));
    SetNodeByYPath(
        orchidRoot,
        "/restart_manager",
        CreateVirtualNode(restartManager->GetOrchidService()));

    RpcProxyHeapUsageProfiler_ = New<TRpcProxyHeapUsageProfiler>(
        GetControlInvoker(),
        Config_->HeapProfiler);
}

void TBootstrap::ReconfigureMemoryLimits(const TProxyMemoryLimitsPtr& memoryLimits)
{
    if (memoryLimits->Total) {
        MemoryUsageTracker_->SetTotalLimit(*memoryLimits->Total);
    }

    const auto& staticLimits = Config_->MemoryLimits;
    auto totalLimit = MemoryUsageTracker_->GetTotalLimit();

    MemoryUsageTracker_->SetCategoryLimit(
        EMemoryCategory::Lookup,
        memoryLimits->Lookup.value_or(staticLimits->Lookup.value_or(totalLimit)));
    MemoryUsageTracker_->SetCategoryLimit(
        EMemoryCategory::Query,
        memoryLimits->Query.value_or(staticLimits->Query.value_or(totalLimit)));
    MemoryUsageTracker_->SetCategoryLimit(
        EMemoryCategory::Rpc,
        memoryLimits->Rpc.value_or(staticLimits->Rpc.value_or(totalLimit)));
}

void TBootstrap::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    ReconfigureNativeSingletons(Config_, newConfig);

    TraceSampler_->UpdateConfig(newConfig->Tracing);

    Connection_->Reconfigure(newConfig->ClusterConnection);

    ApiService_->OnDynamicConfigChanged(newConfig->Api);

    RpcServer_->OnDynamicConfigChanged(newConfig->RpcServer);

    DiskManagerProxy_->OnDynamicConfigChanged(newConfig->DiskManagerProxy);

    ReconfigureMemoryLimits(newConfig->MemoryLimits);
}

const IInvokerPtr& TBootstrap::GetWorkerInvoker() const
{
    return WorkerPool_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
