#include "bootstrap.h"

#include "access_checker.h"
#include "bundle_dynamic_config_manager.h"
#include "config.h"
#include "discovery_service.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "query_corpus_reporter.h"

#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/profilers.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>
#include <yt/yt/server/lib/rpc_proxy/security_manager.h>

#include <yt/yt/server/lib/signature/instance_config.h>
#include <yt/yt/server/lib/signature/key_rotator.h>
#include <yt/yt/server/lib/signature/signature_generator.h>
#include <yt/yt/server/lib/signature/signature_validator.h>

#include <yt/yt/server/lib/signature/key_stores/cypress.h>

#include <yt/yt/server/lib/shuffle_server/shuffle_service.h>

#include <yt/yt/server/lib/admin/admin_service.h>
#include <yt/yt/server/lib/admin/restart_service.h>

#include <yt/yt/server/lib/misc/address_helpers.h>
#include <yt/yt/server/lib/misc/restart_manager.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/transaction_client/config.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/disk_manager/hotswap_manager.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/auth_server/authentication_manager.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/core/misc/configurable_singleton_def.h>

namespace NYT::NRpcProxy {

using namespace NAdmin;
using namespace NApi;
using namespace NAuth;
using namespace NBus;
using namespace NConcurrency;
using namespace NLogging;
using namespace NMonitoring;
using namespace NNet;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NShuffleServer;
using namespace NSignature;
using namespace NYTree;
using namespace NFusion;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = RpcProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TProxyBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ServiceLocator_(std::move(serviceLocator))
    , ControlQueue_(New<TActionQueue>("Control"))
    , WorkerPool_(CreateThreadPool(Config_->WorkerThreadPoolSize, "Worker"))
    , HttpPoller_(CreateThreadPoolPoller(1, "HttpPoller"))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), Config_);
    } else {
        WarnForUnrecognizedOptions(Logger(), Config_);
    }

    if (!Config_->ClusterConnection) {
        THROW_ERROR_EXCEPTION("Cluster connection is missing");
    }
}

TBootstrap::~TBootstrap() = default;

TFuture<void> TBootstrap::Run()
{
    return BIND(&TBootstrap::DoRun, MakeStrong(this))
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run();
}

void TBootstrap::DoRun()
{
    DoInitialize();
    DoStart();
}

void TBootstrap::DoInitialize()
{
    LocalAddresses_ = GetLocalAddresses(Config_->Addresses, Config_->RpcPort);

    YT_LOG_INFO("Starting proxy (LocalAddresses: %v)",
        GetValues(LocalAddresses_));

    MemoryUsageTracker_ = CreateNodeMemoryTracker(
        *Config_->MemoryLimits->Total,
        /*limits*/ {},
        Logger(),
        RpcProxyProfiler().WithPrefix("/memory_usage"));

    ReconfigureMemoryLimits(Config_->MemoryLimits);

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.ConnectionInvoker = GetWorkerInvoker();
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;
    Connection_ = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        std::move(connectionOptions),
        /*clusterDirectoryOverride*/ {},
        MemoryUsageTracker_);

    Connection_->GetClusterDirectorySynchronizer()->Start();
    Connection_->GetNodeDirectorySynchronizer()->Start();
    Connection_->GetQueueConsumerRegistrationManager()->StartSync();
    Connection_->GetMasterCellDirectorySynchronizer()->Start();

    NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

    RootClient_ = Connection_->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::RootUserName));

    NLogging::GetDynamicTableLogWriterFactory()->SetClient(RootClient_);

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

    if (Config_->SignatureValidation) {
        CypressKeyReader_ = New<TCypressKeyReader>(
            Config_->SignatureValidation->CypressKeyReader,
            RootClient_);
        SignatureValidator_ = New<TSignatureValidator>(
            Config_->SignatureValidation->Validator,
            CypressKeyReader_);
    }

    if (Config_->SignatureGeneration) {
        CypressKeyWriter_ = New<TCypressKeyWriter>(
            Config_->SignatureGeneration->CypressKeyWriter,
            RootClient_);
        SignatureGenerator_ = New<TSignatureGenerator>(
            Config_->SignatureGeneration->Generator,
            CypressKeyWriter_);
        SignatureKeyRotator_ = New<TKeyRotator>(
            Config_->SignatureGeneration->KeyRotator,
            GetControlInvoker(),
            SignatureGenerator_);

        YT_UNUSED_FUTURE(CypressKeyWriter_->Initialize());
        SignatureKeyRotator_->Start();
    }

    ProxyCoordinator_ = CreateProxyCoordinator();
    TraceSampler_ = New<NTracing::TSampler>();

    DynamicConfigManager_ = CreateDynamicConfigManager(
        Config_,
        ProxyCoordinator_,
        Connection_,
        GetControlInvoker());
    BundleDynamicConfigManager_ = CreateBundleDynamicConfigManager(
        Config_,
        ProxyCoordinator_,
        Connection_,
        GetControlInvoker());
    AccessChecker_ = CreateAccessChecker(
        Config_->AccessChecker,
        ProxyCoordinator_,
        Connection_,
        DynamicConfigManager_);

    BusServer_ = CreateBusServer(
        Config_->BusServer,
        GetYTPacketTranscoderFactory(),
        MemoryUsageTracker_->WithCategory(EMemoryCategory::Rpc));
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

    // Cycles are fine for bootstrap.
    DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, MakeStrong(this)));
    BundleDynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnBundleDynamicConfigChanged, MakeStrong(this)));

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());
}

void TBootstrap::DoStart()
{
    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
        &MonitoringManager_,
        &orchidRoot);
    NProfiling::TSolomonRegistry::Get()->SetDynamicTags({NProfiling::TTag{"proxy_role", DefaultRpcProxyRole}});

    if (Config_->ExposeConfigInOrchid) {
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
            "/bundle_dynamic_config_manager",
            CreateVirtualNode(BundleDynamicConfigManager_->GetOrchidService()));
        SetNodeByYPath(
            orchidRoot,
            "/cluster_connection",
            CreateVirtualNode(Connection_->GetOrchidService()));
    }
    if (auto hotswapManager = ServiceLocator_->FindService<NDiskManager::IHotswapManagerPtr>()) {
        SetNodeByYPath(
            orchidRoot,
            "/disk_monitoring",
            CreateVirtualNode(hotswapManager->GetOrchidService()));
    }
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
        Logger());

    QueryCorpusReporter_ = MakeQueryCorpusReporter(RootClient_);

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
            RpcProxyLogger(),
            RpcProxyProfiler(),
            (SignatureValidator_ ? SignatureValidator_ : CreateAlwaysThrowingSignatureValidator()),
            (SignatureGenerator_ ? SignatureGenerator_ : CreateAlwaysThrowingSignatureGenerator()),
            MemoryUsageTracker_,
            /*stickyTransactionPool*/ {},
            QueryCorpusReporter_);
    };

    ApiService_ = createApiService(AuthenticationManager_);
    if (TvmOnlyAuthenticationManager_) {
        TvmOnlyApiService_ = createApiService(TvmOnlyAuthenticationManager_);
    }

    RpcServer_->RegisterService(ApiService_);
    if (TvmOnlyRpcServer_ && TvmOnlyApiService_) {
        TvmOnlyRpcServer_->RegisterService(TvmOnlyApiService_);
    }

    if (Config_->EnableShuffleService) {
        auto localServerAddress = BuildServiceAddress(GetLocalHostName(), Config_->RpcPort);
        ShuffleService_ = CreateShuffleService(
            GetWorkerInvoker(),
            RootClient_,
            localServerAddress);
        RpcServer_->RegisterService(ShuffleService_);
        Connection_->RegisterShuffleService(localServerAddress);
    }

    DynamicConfigManager_->Initialize();
    DynamicConfigManager_->Start();

    BundleDynamicConfigManager_->Initialize();
    BundleDynamicConfigManager_->Start();

    // NB: We must apply the first dynamic config before ApiService_ starts.
    YT_LOG_INFO("Loading dynamic config for the first time");

    {
        auto error = WaitFor(DynamicConfigManager_->GetConfigLoadedFuture());
        YT_LOG_FATAL_UNLESS(
            error.IsOK(),
            error,
            "Unexpected failure while waiting for the first dynamic config loaded");
    }

    YT_LOG_INFO("Dynamic config loaded");

    QueryCorpusReporter_->Reconfigure(DynamicConfigManager_->GetConfig()->Api->QueryCorpusReporter);

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
        ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
        NativeAuthenticator_);
    RpcServer_->RegisterService(adminService);
    if (TvmOnlyRpcServer_) {
        TvmOnlyRpcServer_->RegisterService(adminService);
    }

    auto restartManager = New<TRestartManager>(GetControlInvoker());
    RpcServer_->RegisterService(CreateRestartService(
        restartManager,
        GetControlInvoker(),
        RpcProxyLogger(),
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

    RpcProxyHeapUsageProfiler_ = New<TProxyHeapUsageProfiler>(
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

void TBootstrap::ReconfigureConnection(
    const TProxyDynamicConfigPtr& dynamicConfig,
    const TBundleProxyDynamicConfigPtr& bundleConfig)
{
    auto connectionConfig = CloneYsonStruct(dynamicConfig->ClusterConnection);
    const auto& clockManagerConfig = connectionConfig->ClockManager;
    if (bundleConfig->ClockClusterTag) {
        clockManagerConfig->ClockClusterTag = bundleConfig->ClockClusterTag;
    }

    Connection_->Reconfigure(connectionConfig);
}

void TBootstrap::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    TSingletonManager::Reconfigure(newConfig);

    TraceSampler_->UpdateConfig(newConfig->Tracing);

    ApiService_->OnDynamicConfigChanged(newConfig->Api);

    RpcServer_->OnDynamicConfigChanged(newConfig->RpcServer);

    QueryCorpusReporter_->Reconfigure(newConfig->Api->QueryCorpusReporter);

    ReconfigureMemoryLimits(newConfig->MemoryLimits);

    ReconfigureConnection(newConfig, BundleDynamicConfigManager_->GetConfig());
}

void TBootstrap::OnBundleDynamicConfigChanged(
    const TBundleProxyDynamicConfigPtr& /*oldConfig*/,
    const TBundleProxyDynamicConfigPtr& newConfig)
{
    ReconfigureConnection(DynamicConfigManager_->GetConfig(), newConfig);
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

TBootstrapPtr CreateRpcProxyBootstrap(
    TProxyBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
