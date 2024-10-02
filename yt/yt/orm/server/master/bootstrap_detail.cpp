#include "bootstrap_detail.h"

#include "config.h"
#include "helpers.h"
#include "private.h"
#include "yt_connector.h"

#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/access_control/data_model_interop.h>
#include <yt/yt/orm/server/access_control/helpers.h>

#include <yt/yt/orm/server/master/event_log.h>

#include <yt/yt/orm/server/objects/object_manager.h>
#include <yt/yt/orm/server/objects/pool_weights_manager.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/watch_log_consumer_interop.h>
#include <yt/yt/orm/server/objects/watch_manager.h>

#include <yt/yt/server/lib/rpc_proxy/access_checker.h>
#include <yt/yt/server/lib/rpc_proxy/api_service.h>
#include <yt/yt/server/lib/rpc_proxy/proxy_coordinator.h>
#include <yt/yt/server/lib/rpc_proxy/security_manager.h>

#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/library/auth_server/authentication_manager.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/tracing/jaeger/sampler.h>

#include <yt/yt/client/api/sticky_transaction_pool.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/new_fair_share_thread_pool.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/https/server.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/grpc/server.h>

#include <yt/yt/core/rpc/http/server.h>

#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>

#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOrm::NServer::NMaster {

using namespace NAuth;
using namespace NConcurrency;
using namespace NNet;
using namespace NNodeTrackerClient;
using namespace NYTree;

using namespace NOrm::NServer::NObjects;
using namespace NOrm::NServer::NAccessControl;

////////////////////////////////////////////////////////////////////////////////

namespace {

YT_DEFINE_GLOBAL(const NLogging::TLogger, RpcProxyLogger, "RpcProxy");
inline const NProfiling::TProfiler RpcProxyProfiler("/rpc_proxy");
inline const NProfiling::TProfiler TvmServiceProfiler("/auth/tvm/remove");

} // namespace

////////////////////////////////////////////////////////////////////////////////

TBootstrapBase::TBootstrapBase(TMasterConfigPtr config, INodePtr configNode)
    : InitialConfigNode_(std::move(configNode))
    , InitialConfig_(std::move(config))
    , ControlQueue_(New<TActionQueue>("Control"))
    , PoolWeightManager_(CreatePoolWeightManager(this, InitialConfig_->PoolWeightManager))
    , WorkerPool_(CreateNewTwoLevelFairShareThreadPool(
        InitialConfig_->WorkerThreadPoolSize,
        "Worker",
        {.PoolWeightProvider = PoolWeightManager_}))
{
    SubscribeConfigUpdate(BIND(&TBootstrapBase::OnConfigUpdate, this));
}

////////////////////////////////////////////////////////////////////////////////

const NYT::NApi::IStickyTransactionPoolPtr& TBootstrapBase::GetUnderlyingTransactionPool() const
{
    return UnderlyingTransactionPool_;
}

const TYTConnectorPtr& TBootstrapBase::GetYTConnector() const
{
    return YTConnector_;
}

const TObjectManagerPtr& TBootstrapBase::GetObjectManager() const
{
    return ObjectManager_;
}

const TAccessControlManagerPtr& TBootstrapBase::GetAccessControlManager() const
{
    return AccessControlManager_;
}

const IDataModelInteropPtr& TBootstrapBase::GetAccessControlDataModelInterop() const
{
    return AccessControlDataModelInterop_;
}

const TTransactionManagerPtr& TBootstrapBase::GetTransactionManager() const
{
    return TransactionManager_;
}

const IPoolWeightManagerPtr& TBootstrapBase::GetPoolWeightManager() const
{
    return PoolWeightManager_;
}

const TWatchManagerPtr& TBootstrapBase::GetWatchManager() const
{
    return WatchManager_;
}

const NObjects::IWatchLogConsumerInteropPtr& TBootstrapBase::GetWatchLogConsumerInterop() const
{
    return WatchLogConsumerInterop_;
}

const ITvmServicePtr& TBootstrapBase::GetTvmService() const
{
    return TvmService_;
}

const IAuthenticationManagerPtr& TBootstrapBase::GetAuthenticationManager() const
{
    return AuthenticationManager_;
}

////////////////////////////////////////////////////////////////////////////////

const IInvokerPtr& TBootstrapBase::GetControlInvoker()
{
    return ControlQueue_->GetInvoker();
}

IInvokerPtr TBootstrapBase::GetWorkerPoolInvoker()
{
    if (AccessControlManager_) {
        if (auto identity = TryGetAuthenticatedUserIdentity()) {
            auto pool = SelectExecutionPoolTag(
                AccessControlManager_,
                identity->User,
                /*userTagInsteadOfPool*/ true);
            return GetWorkerPoolInvoker(*pool, identity->UserTag);
        }
    }
    return GetWorkerPoolInvoker("default", "");
}

IInvokerPtr TBootstrapBase::GetWorkerPoolInvoker(const std::string& poolName, const std::string& tag)
{
    if (!GetPoolWeightManager()->IsFairShareEnabled()) {
        return WorkerPool_->GetInvoker("default", "");
    }
    YT_LOG_DEBUG(
        "Invoker selected (PoolName: %v, Tag: %v)",
        poolName,
        tag);
    // TODO(babenko): switch to std::string
    return WorkerPool_->GetInvoker(TString(poolName), TString(tag));
}

////////////////////////////////////////////////////////////////////////////////

const TString& TBootstrapBase::GetDBName() const
{
    return InitialConfig_->DBName;
}

const TString& TBootstrapBase::GetFqdn() const
{
    return InitialConfig_->FqdnOverride.has_value() ? *InitialConfig_->FqdnOverride : Fqdn_;
}

const TString& TBootstrapBase::GetIP6Address() const
{
    return IP6Address_;
}

const TAddressMap& TBootstrapBase::GetInternalRpcAddresses() const
{
    return InternalRpcAddresses_;
}

const TString& TBootstrapBase::GetClientGrpcAddress() const
{
    return ClientGrpcAddress_;
}

const TString& TBootstrapBase::GetClientGrpcIP6Address() const
{
    return ClientGrpcIP6Address_;
}

const TString& TBootstrapBase::GetSecureClientGrpcAddress() const
{
    return SecureClientGrpcAddress_;
}

const TString& TBootstrapBase::GetSecureClientGrpcIP6Address() const
{
    return SecureClientGrpcIP6Address_;
}

const TString& TBootstrapBase::GetClientHttpAddress() const
{
    return ClientHttpAddress_;
}

const TString& TBootstrapBase::GetClientHttpIP6Address() const
{
    return ClientHttpIP6Address_;
}

const TString& TBootstrapBase::GetSecureClientHttpAddress() const
{
    return SecureClientHttpAddress_;
}

const TString& TBootstrapBase::GetSecureClientHttpIP6Address() const
{
    return SecureClientHttpIP6Address_;
}

const TString& TBootstrapBase::GetMonitoringAddress() const
{
    return MonitoringAddress_;
}

const TString& TBootstrapBase::GetRpcProxyAddress() const
{
    return RpcProxyAddress_;
}

const TString& TBootstrapBase::GetRpcProxyIP6Address() const
{
    return RpcProxyIP6Address_;
}

const NYT::NTracing::TSamplerPtr& TBootstrapBase::GetTracingSampler() const
{
    return TracingSampler_;
}

const NHttp::IServerPtr& TBootstrapBase::GetHttpMonitoringServer()
{
    return HttpMonitoringServer_;
}

TEventLoggerPtr TBootstrapBase::CreateEventLogger(NLogging::TLogger logger)
{
    YT_VERIFY(EventLogWriter_);
    return New<TEventLogger>(
        std::move(logger),
        GetFqdn(),
        GetTransactionManager(),
        EventLogWriter_);
}

////////////////////////////////////////////////////////////////////////////////

void TBootstrapBase::Start()
{
    BIND(&TBootstrapBase::DoStart, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrapBase::Stop()
{
    BIND(&TBootstrapBase::DoStop, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TString TBootstrapBase::BuildGrpcAddress(const NRpc::NGrpc::TServerConfigPtr& config)
{
    return BuildServiceAddress(Fqdn_, GetGrpcPort(config));
}

const IThreadPoolPollerPtr& TBootstrapBase::GetHttpPoller()
{
    return HttpPoller_;
}

void TBootstrapBase::OnConfigUpdate(const TMasterDynamicConfigPtr& config)
{
    YT_VERIFY(config);

    if (!DynamicConfig_ || !AreConfigsEqual(DynamicConfig_->TracingSampler, config->TracingSampler)) {
        TracingSampler_->UpdateConfig(config->TracingSampler);
    }

    if (IP6ValidationExecutor_) {
        IP6ValidationExecutor_->SetPeriod(config->IP6AddressValidationPeriod);
    }

    YT_VERIFY(EventLogWriter_);
    EventLogWriter_->UpdateConfig(config->EventLogManager->AsYTEventLogManagerConfig());

    DynamicConfig_ = config;
}

int TBootstrapBase::GetGrpcPort(const NRpc::NGrpc::TServerConfigPtr& config)
{
    TStringBuf dummyHostName;
    int grpcPort;
    ParseServiceAddress(config->Addresses[0]->Address, &dummyHostName, &grpcPort);
    return grpcPort;
}

TString TBootstrapBase::BuildGrpcIP6Address(const NRpc::NGrpc::TServerConfigPtr& config)
{
    return Format("[%v]:%v", IP6Address_, GetGrpcPort(config));
}

TString TBootstrapBase::BuildHttpAddress(const NHttp::TServerConfigPtr& config)
{
    return BuildServiceAddress(Fqdn_, config->Port);
}

TString TBootstrapBase::BuildHttpIP6Address(const NHttp::TServerConfigPtr& config)
{
    return Format("[%v]:%v", IP6Address_, config->Port);
}

TString TBootstrapBase::BuildInternalRpcAddress(const NBus::TBusServerConfigPtr& config)
{
    return BuildServiceAddress(Fqdn_, *config->Port);
}

TString TBootstrapBase::BuildInternalRpcIP6Address(const NBus::TBusServerConfigPtr& config)
{
    return Format("[%v]:%v", IP6Address_, *config->Port);
}

TString TBootstrapBase::ResolveIP6Address() const
{
    return ToString(NNet::TAddressResolver::Get()->Resolve(Fqdn_)
        .Get()
        .ValueOrThrow()
        .ToIP6Address());
}

void TBootstrapBase::ValidateIP6Address() const
{
    auto validationEnabled = DynamicConfig_
        ? DynamicConfig_->EnableIP6AddressValidation
        : InitialConfig_->EnableIP6AddressValidation;
    if (!validationEnabled) {
        return;
    }

    // Fails fast if ip6 address changed for some reason.
    try {
        auto address = ResolveIP6Address();
        YT_VERIFY(address == IP6Address_);
    } catch (...) {
        IP6ResolutionFailures_.Increment();
    }
}

void TBootstrapBase::DoStart()
{
    YT_LOG_INFO("Processing configuration");
    Fqdn_ = GetLocalHostName();
    if (InitialConfig_->EnableIP6AddressResolving ||
        InitialConfig_->IP6AddressOverride.has_value())
    {
        if (InitialConfig_->IP6AddressOverride.has_value()) {
            IP6Address_ = *InitialConfig_->IP6AddressOverride;
        } else {
            IP6Address_ = ResolveIP6Address();
            IP6ValidationExecutor_ = New<TPeriodicExecutor>(
                GetControlInvoker(),
                BIND(&TBootstrapBase::ValidateIP6Address, this),
                InitialConfig_->IP6AddressValidationPeriod);
        }

        if (InitialConfig_->ClientHttpServer) {
            ClientHttpIP6Address_ = BuildHttpIP6Address(InitialConfig_->ClientHttpServer);
        }
        if (InitialConfig_->SecureClientHttpServer) {
            SecureClientHttpIP6Address_ = BuildHttpIP6Address(InitialConfig_->SecureClientHttpServer);
        }
        if (InitialConfig_->ClientGrpcServer) {
            ClientGrpcIP6Address_ = BuildGrpcIP6Address(InitialConfig_->ClientGrpcServer);
        }
        if (InitialConfig_->SecureClientGrpcServer) {
            SecureClientGrpcIP6Address_ = BuildGrpcIP6Address(InitialConfig_->SecureClientGrpcServer);
        }
        if (InitialConfig_->RpcProxyCollocation) {
            YT_VERIFY(InitialConfig_->InternalBusServer);
            RpcProxyIP6Address_ = BuildInternalRpcIP6Address(InitialConfig_->InternalBusServer);
        }
    }
    if (InitialConfig_->InternalBusServer) {
        EmplaceOrCrash(InternalRpcAddresses_,
            DefaultNetworkName,
            BuildInternalRpcAddress(InitialConfig_->InternalBusServer));
    }
    if (InitialConfig_->ClientGrpcServer) {
        ClientGrpcAddress_ = BuildGrpcAddress(InitialConfig_->ClientGrpcServer);
    }
    if (InitialConfig_->SecureClientGrpcServer) {
        SecureClientGrpcAddress_ = BuildGrpcAddress(InitialConfig_->SecureClientGrpcServer);
    }
    if (InitialConfig_->ClientHttpServer) {
        ClientHttpAddress_ = BuildHttpAddress(InitialConfig_->ClientHttpServer);
    }
    if (InitialConfig_->SecureClientHttpServer) {
        SecureClientHttpAddress_ = BuildHttpAddress(InitialConfig_->SecureClientHttpServer);
    }
    if (InitialConfig_->RpcProxyCollocation) {
        YT_VERIFY(InitialConfig_->InternalBusServer);
        RpcProxyAddress_ = BuildInternalRpcAddress(InitialConfig_->InternalBusServer);
    }
    // TODO(dgolear): YT-18249, unify tvm services?
    if (!InitialConfig_->Jaeger->TvmService && InitialConfig_->AuthenticationManager->TvmService) {
        InitialConfig_->Jaeger->TvmService = InitialConfig_->AuthenticationManager->TvmService;
    }
    TracingSampler_ = New<NTracing::TSampler>(InitialConfig_->TracingSampler);
    NTracing::SetGlobalTracer(New<NTracing::TJaegerTracer>(InitialConfig_->Jaeger));

    YT_LOG_INFO("Processing data model configuration");
    ProcessConfig();
    YT_LOG_INFO("Processed configuration (Fqdn: %v)",
        Fqdn_);

    YT_LOG_INFO("Creating components");
    // Uses RPC proxy logger, because it is managed by RPC proxy solely for now.
    UnderlyingTransactionPool_ = NYT::NApi::CreateStickyTransactionPool(RpcProxyLogger());
    HttpPoller_ = CreateThreadPoolPoller(InitialConfig_->HttpPollerThreadPoolSize, "Http");
    if (InitialConfig_->TvmService) {
        TvmService_ = CreateTvmService(InitialConfig_->TvmService, TvmServiceProfiler);
    }
    YTConnector_ = CreateYTConnector();
    ObjectManager_ = CreateObjectManager();
    TransactionManager_ = CreateTransactionManager();
    WatchManager_ = New<TWatchManager>(this, InitialConfig_->WatchManager);
    WatchLogConsumerInterop_ = CreateWatchLogConsumerInterop();
    AccessControlManager_ = CreateAccessControlManager();
    AccessControlDataModelInterop_ = CreateAccessControlDataModelInterop();
    AuthenticationManager_ = CreateAuthenticationManager(
        InitialConfig_->AuthenticationManager,
        HttpPoller_,
        YTConnector_->GetControlClient(),
        TvmService_);
    EventLogWriter_ = CreateDynamicTableEventLogWriter(
        InitialConfig_->EventLogManager,
        InitialConfig_->EventLogPath,
        YTConnector_->GetClient(YTConnector_->FormatUserTag()),
        GetControlInvoker());
    YT_LOG_INFO("Creating data model components");
    CreateComponents();

    YT_LOG_INFO("Initializing components");
    YTConnector_->Initialize();
    TransactionManager_->Initialize();
    ObjectManager_->Initialize();
    WatchManager_->Initialize();
    AccessControlManager_->Initialize();
    InitializeDynamicConfigManager();
    YT_LOG_INFO("Initializing data model components");
    InitializeComponents();

    YT_LOG_INFO("Creating services");
    ObjectService_ = CreateObjectService();
    ClientDiscoveryService_ = CreateClientDiscoveryService();
    SecureClientDiscoveryService_ = CreateSecureClientDiscoveryService();
    YT_LOG_INFO("Creating data model services");
    CreateServices();

    YT_LOG_INFO("Creating servers");
    if (InitialConfig_->MonitoringServer) {
        InitialConfig_->MonitoringServer->ServerName = "Monitoring";
        HttpMonitoringServer_ = NHttp::CreateServer(
            InitialConfig_->MonitoringServer,
            HttpPoller_);

        HttpMonitoringServer_->AddHandler(
            "/health_check",
            BIND(&TBootstrapBase::HealthCheckHandler, this));

        MonitoringAddress_ = BuildHttpAddress(InitialConfig_->MonitoringServer);
    }
    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpMonitoringServer_,
        InitialConfig_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);
    YT_LOG_INFO("Setting up data model Orchid");
    SetupOrchid(orchidRoot);
    SetupSensors();

    if (InitialConfig_->InternalBusServer) {
        InternalBusServer_ = NBus::CreateBusServer(InitialConfig_->InternalBusServer);
    }
    if (InitialConfig_->InternalRpcServer && InternalBusServer_) {
        InternalRpcServer_ = NRpc::NBus::CreateBusServer(InternalBusServer_);
        InternalRpcServer_->RegisterService(NOrchid::CreateOrchidService(
            orchidRoot,
            GetControlInvoker(),
            /*authenticator*/ nullptr));
        if (InitialConfig_->RpcProxyCollocation) {
            auto securityManager = NRpcProxy::CreateSecurityManager(
                InitialConfig_->RpcProxyCollocation->ApiService->SecurityManager,
                GetYTConnector()->GetNativeConnection(),
                RpcProxyLogger());

            auto traceSampler = New<NTracing::TSampler>();

            auto proxyCoordinator = NRpcProxy::CreateProxyCoordinator();
            proxyCoordinator->SetAvailableState(true);

            auto accessChecker = NRpcProxy::CreateNoopAccessChecker();

            auto apiService = NRpcProxy::CreateApiService(
                InitialConfig_->RpcProxyCollocation->ApiService,
                GetControlInvoker(),
                GetWorkerPoolInvoker(),
                GetYTConnector()->GetNativeConnection(),
                AuthenticationManager_->GetRpcAuthenticator(),
                proxyCoordinator,
                accessChecker,
                securityManager,
                traceSampler,
                RpcProxyLogger(),
                RpcProxyProfiler,
                /*memoryUsageTracker*/ nullptr,
                UnderlyingTransactionPool_);
            apiService->OnDynamicConfigChanged(InitialConfig_->RpcProxyCollocation->ApiServiceDynamic);
            InternalRpcServer_->RegisterService(std::move(apiService));
        }
        InternalRpcServer_->Configure(InitialConfig_->InternalRpcServer);
    }
    if (InitialConfig_->ClientHttpServer) {
        InitialConfig_->ClientHttpServer->ServerName = "Client";
        auto clientHttpServer = NHttp::CreateServer(
            InitialConfig_->ClientHttpServer,
            HttpPoller_);
        clientHttpServer->AddHandler(
            "/health_check",
            BIND(&TBootstrapBase::HealthCheckHandler, this));

        ClientHttpRpcServer_ = NRpc::NHttp::CreateServer(clientHttpServer);
        ClientHttpRpcServer_->RegisterService(ObjectService_);
        if (ClientDiscoveryService_) {
            ClientHttpRpcServer_->RegisterService(ClientDiscoveryService_);
        }
    }
    if (InitialConfig_->SecureClientHttpServer) {
        InitialConfig_->SecureClientHttpServer->ServerName = "SecureClient";
        auto secureClientHttpServer = NHttps::CreateServer(
            InitialConfig_->SecureClientHttpServer,
            HttpPoller_);
        secureClientHttpServer->AddHandler(
            "/health_check",
            BIND(&TBootstrapBase::HealthCheckHandler, this));
        SecureClientHttpRpcServer_ = NRpc::NHttp::CreateServer(secureClientHttpServer);
        SecureClientHttpRpcServer_->RegisterService(ObjectService_);
        if (SecureClientDiscoveryService_) {
            SecureClientHttpRpcServer_->RegisterService(SecureClientDiscoveryService_);
        }
    }
    if (InitialConfig_->ClientGrpcServer) {
        InitialConfig_->ClientGrpcServer->ProfilingName = "insecure";
        ClientGrpcServer_ = NRpc::NGrpc::CreateServer(InitialConfig_->ClientGrpcServer);
        ClientGrpcServer_->RegisterService(ObjectService_);
        if (ClientDiscoveryService_) {
            ClientGrpcServer_->RegisterService(ClientDiscoveryService_);
        }
        YT_LOG_INFO("Registering client GRPC server data model services");
        RegisterClientGrpcServerServices(ClientGrpcServer_);
    }
    if (InitialConfig_->SecureClientGrpcServer) {
        InitialConfig_->SecureClientGrpcServer->ProfilingName = "secure";
        SecureClientGrpcServer_ = NRpc::NGrpc::CreateServer(InitialConfig_->SecureClientGrpcServer);
        SecureClientGrpcServer_->RegisterService(ObjectService_);
        if (SecureClientDiscoveryService_) {
            SecureClientGrpcServer_->RegisterService(SecureClientDiscoveryService_);
        }
    }
    YT_LOG_INFO("Creating data model servers");
    CreateServers();

    YT_LOG_INFO("Starting components");
    YTConnector_->Start();
    StartDynamicConfigManager();
    YT_LOG_INFO("Starting data model components");
    StartComponents();

    if (IP6ValidationExecutor_) {
        IP6ValidationExecutor_->Start();
    }

    YT_LOG_INFO("Starting servers");
    if (HttpMonitoringServer_) {
        HttpMonitoringServer_->Start();
    }
    if (InternalRpcServer_) {
        InternalRpcServer_->Start();
    }
    if (ClientHttpRpcServer_) {
        ClientHttpRpcServer_->Start();
    }
    if (SecureClientHttpRpcServer_) {
        SecureClientHttpRpcServer_->Start();
    }
    if (ClientGrpcServer_) {
        ClientGrpcServer_->Start();
    }
    if (SecureClientGrpcServer_) {
        SecureClientGrpcServer_->Start();
    }
    YT_LOG_INFO("Starting data model servers");
    StartServers();

    YT_LOG_INFO("Master is running");
}

void TBootstrapBase::DoStop()
{
    YT_LOG_INFO("Stopping data model servers");
    StopServers();

    YT_LOG_INFO("Stopping components");
    if (SecureClientGrpcServer_) {
        YT_LOG_DEBUG("Stopping secure grpc server");
        if (auto error = SecureClientGrpcServer_->Stop().GetUnique(); !error.IsOK()) {
            YT_LOG_ERROR(error, "Failed to stop secure grpc server");
        }
    }
    if (ClientGrpcServer_) {
        YT_LOG_DEBUG("Stopping grpc server");
        if (auto error = ClientGrpcServer_->Stop().GetUnique(); !error.IsOK()) {
            YT_LOG_ERROR(error, "Failed to stop grpc server");
        }
    }
    if (SecureClientHttpRpcServer_) {
        YT_LOG_DEBUG("Stopping secure client http rpc server");
        if (auto error = SecureClientHttpRpcServer_->Stop().GetUnique(); !error.IsOK()) {
            YT_LOG_ERROR(error, "Failed to stop secure client http rpc server");
        }
    }
    if (ClientHttpRpcServer_) {
        YT_LOG_DEBUG("Stopping client http rpc server");
        if (auto error = ClientHttpRpcServer_->Stop().GetUnique(); !error.IsOK()) {
            YT_LOG_ERROR(error, "Failed to stop client http rpc server");
        }
    }
    if (InternalRpcServer_) {
        YT_LOG_DEBUG("Stopping internal rpc server");
        if (auto error = InternalRpcServer_->Stop().GetUnique(); !error.IsOK()) {
            YT_LOG_ERROR(error, "Failed to stop internal rpc server");
        }
    }
    if (HttpMonitoringServer_) {
        YT_LOG_DEBUG("Stopping http monitoring server");
        HttpMonitoringServer_->Stop();
    }
    if (MonitoringManager_) {
        YT_LOG_DEBUG("Stopping monitoring");
        MonitoringManager_->Stop();
    }
    StopComponents();
    StopDynamicConfigManager();
    if (auto tracer = NYT::NTracing::GetGlobalTracer()) {
        YT_LOG_DEBUG("Stopping jaeger tracer");
        tracer->Stop();
    }

    YT_LOG_INFO("Master is stopped");
}

void TBootstrapBase::SetupSensors()
{
    Profiler.AddFuncGauge("/is_connected", YTConnector_, [this] {
        return YTConnector_->IsConnected();
    });
    Profiler.AddFuncGauge("/is_leading", YTConnector_, [this] {
        return YTConnector_->IsLeading();
    });
    Profiler.AddFuncGauge("/is_alive", YTConnector_, [this] {
        return IsAlive();
    });

    IP6ResolutionFailures_ = Profiler.Counter("/ip6_resolution_failures");
}

bool TBootstrapBase::IsAlive() const
{
    auto isConnected = YTConnector_->IsConnected();
    auto snapshotsPreloaded = AccessControlManager_->AccessControlSnapshotsPreloaded();
    YT_LOG_DEBUG("Checking master liveness (IsConnected: %v, SnapshotsPreloaded: %v)",
        isConnected,
        snapshotsPreloaded);
    return isConnected && snapshotsPreloaded;
}

void TBootstrapBase::HealthCheckHandler(
    const NHttp::IRequestPtr& /*req*/,
    const NHttp::IResponseWriterPtr& rsp) const
{
    rsp->SetStatus(IsAlive()
        ? NHttp::EStatusCode::OK
        : NHttp::EStatusCode::ServiceUnavailable);
    WaitFor(rsp->Close())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

IAttributeDictionaryPtr TBootstrapBase::GetInstanceDynamicAttributes() const
{
    return CreateEphemeralAttributes();
}

////////////////////////////////////////////////////////////////////////////////

const INodePtr& TBootstrapBase::GetInitialConfigNode() const
{
    return InitialConfigNode_;
}

const TMasterConfigPtr& TBootstrapBase::GetInitialConfig() const
{
    return InitialConfig_;
}

////////////////////////////////////////////////////////////////////////////////

void TBootstrapBase::ProcessConfig()
{ }

////////////////////////////////////////////////////////////////////////////////

void TBootstrapBase::CreateComponents()
{ }

void TBootstrapBase::InitializeComponents()
{ }

void TBootstrapBase::InitializeDynamicConfigManager()
{ }

////////////////////////////////////////////////////////////////////////////////

void TBootstrapBase::CreateServices()
{ }

////////////////////////////////////////////////////////////////////////////////

void TBootstrapBase::RegisterClientGrpcServerServices(NRpc::IServerPtr /*server*/)
{ }

void TBootstrapBase::CreateServers()
{ }

////////////////////////////////////////////////////////////////////////////////

void TBootstrapBase::StartDynamicConfigManager()
{ }

void TBootstrapBase::StopDynamicConfigManager()
{ }

void TBootstrapBase::StartComponents()
{ }

void TBootstrapBase::StopComponents()
{ }

void TBootstrapBase::StartServers()
{ }

void TBootstrapBase::StopServers()
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
