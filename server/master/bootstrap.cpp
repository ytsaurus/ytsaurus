#include "bootstrap.h"
#include "config.h"
#include "yt_connector.h"
#include "private.h"

#include <yp/server/nodes/node_tracker_service.h>
#include <yp/server/nodes/node_tracker.h>

#include <yp/server/api/object_service.h>
#include <yp/server/api/discovery_service.h>

#include <yp/server/objects/object_manager.h>
#include <yp/server/objects/watch_manager.h>
#include <yp/server/objects/transaction_manager.h>

#include <yp/server/net/net_manager.h>

#include <yp/server/scheduler/resource_manager.h>
#include <yp/server/scheduler/scheduler.h>

#include <yp/server/access_control/access_control_manager.h>

#include <yp/server/accounting/accounting_manager.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/monitoring/monitoring_manager.h>
#include <yt/ytlib/monitoring/http_integration.h>

#include <yt/ytlib/auth/authentication_manager.h>
#include <yt/ytlib/auth/default_secret_vault_service.h>
#include <yt/ytlib/auth/batching_secret_vault_service.h>
#include <yt/ytlib/auth/caching_secret_vault_service.h>
#include <yt/ytlib/auth/dummy_secret_vault_service.h>

#include <yt/ytlib/api/native/client.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/core/http/server.h>

#include <yt/core/https/server.h>

#include <yt/core/rpc/http/server.h>

#include <yt/core/rpc/grpc/server.h>

#include <yt/core/rpc/server.h>

#include <yt/core/rpc/bus/server.h>

#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/core/ytalloc/statistics_producer.h>

#include <yt/core/net/local_address.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/virtual.h>

#include <yt/core/profiling/profile_manager.h>

namespace NYP::NServer::NMaster {

using namespace NYT;
using namespace NYT::NConcurrency;
using namespace NYT::NNet;
using namespace NYT::NYTree;
using namespace NYT::NAuth;
using namespace NYT::NNodeTrackerClient;

using namespace NServer::NObjects;
using namespace NServer::NNet;
using namespace NServer::NNodes;
using namespace NServer::NScheduler;
using namespace NServer::NAccessControl;
using namespace NServer::NAccounting;
using namespace NServer::NApi;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap::TImpl
{
public:
    TImpl(TBootstrap* bootstrap, TMasterConfigPtr config, INodePtr configPatchNode)
        : Bootstrap_(bootstrap)
        , InitialConfig_(std::move(config))
        , InitialConfigPatchNode_(std::move(configPatchNode))
        , InitialConfigNode_(ConvertToNode(InitialConfig_))
        , WorkerPool_(New<NConcurrency::TThreadPool>(InitialConfig_->WorkerThreadPoolSize, "Worker"))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(GetControlInvoker(), ControlThread);
    }

    const IInvokerPtr& GetControlInvoker()
    {
        return ControlQueue_->GetInvoker();
    }

    const IInvokerPtr& GetWorkerPoolInvoker()
    {
        return WorkerPool_->GetInvoker();
    }

    const TYTConnectorPtr& GetYTConnector()
    {
        return YTConnector_;
    }

    const TObjectManagerPtr& GetObjectManager()
    {
        return ObjectManager_;
    }

    const TNetManagerPtr& GetNetManager()
    {
        return NetManager_;
    }

    const TTransactionManagerPtr& GetTransactionManager()
    {
        return TransactionManager_;
    }

    const TWatchManagerPtr& GetWatchManager()
    {
        return WatchManager_;
    }

    const TNodeTrackerPtr& GetNodeTracker()
    {
        return NodeTracker_;
    }

    const TSchedulerPtr& GetScheduler()
    {
        return Scheduler_;
    }

    const TResourceManagerPtr& GetResourceManager()
    {
        return ResourceManager_;
    }

    const TAccessControlManagerPtr& GetAccessControlManager()
    {
        return AccessControlManager_;
    }

    const TAccountingManagerPtr& GetAccountingManager()
    {
        return AccountingManager_;
    }

    const TAuthenticationManagerPtr& GetAuthenticationManager()
    {
        return AuthenticationManager_;
    }

    const ISecretVaultServicePtr& GetSecretVaultService()
    {
        return SecretVaultService_;
    }

    const TString& GetFqdn()
    {
        return Fqdn_;
    }

    const TAddressMap& GetInternalRpcAddresses()
    {
        return InternalRpcAddresses_;
    }

    const TString& GetClientGrpcAddress()
    {
        return ClientGrpcAddress_;
    }

    const TString& GetSecureClientGrpcAddress()
    {
        return SecureClientGrpcAddress_;
    }

    const TString& GetClientHttpAddress()
    {
        return ClientHttpAddress_;
    }

    const TString& GetSecureClientHttpAddress()
    {
        return SecureClientHttpAddress_;
    }

    const TString& GetAgentGrpcAddress()
    {
        return AgentGrpcAddress_;
    }

    void Run()
    {
        BIND(&TImpl::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
        Sleep(TDuration::Max());
    }

private:
    TBootstrap* const Bootstrap_;
    const TMasterConfigPtr InitialConfig_;
    //! Represents content of configuration file in the yson format. Generally it differs
    //! from the initial configuration. In particular, it does not contain defaults.
    const INodePtr InitialConfigPatchNode_;
    //! Represents initial configuration in the yson format.
    const INodePtr InitialConfigNode_;

    const TActionQueuePtr ControlQueue_ = New<TActionQueue>("Control");
    const TThreadPoolPtr WorkerPool_;

    TYTConnectorPtr YTConnector_;
    TObjectManagerPtr ObjectManager_;
    TNetManagerPtr NetManager_;
    TTransactionManagerPtr TransactionManager_;
    TWatchManagerPtr WatchManager_;
    TNodeTrackerPtr NodeTracker_;
    TResourceManagerPtr ResourceManager_;
    TAccessControlManagerPtr AccessControlManager_;
    TAccountingManagerPtr AccountingManager_;
    TAuthenticationManagerPtr AuthenticationManager_;
    ISecretVaultServicePtr SecretVaultService_;
    TSchedulerPtr Scheduler_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;

    NRpc::IServicePtr ObjectService_;
    NRpc::IServicePtr ClientDiscoveryService_;
    NRpc::IServicePtr SecureClientDiscoveryService_;
    NRpc::IServicePtr AgentDiscoveryService_;
    NRpc::IServicePtr NodeTrackerService_;

    NConcurrency::IPollerPtr HttpPoller_;

    NHttp::IServerPtr HttpMonitoringServer_;
    NHttp::IServerPtr ClientHttpServer_;
    NHttp::IServerPtr SecureClientHttpServer_;
    NRpc::IServerPtr ClientHttpRpcServer_;
    NRpc::IServerPtr SecureClientHttpRpcServer_;
    NRpc::IServerPtr ClientGrpcServer_;
    NRpc::IServerPtr SecureClientGrpcServer_;
    NRpc::IServerPtr AgentGrpcServer_;
    NBus::IBusServerPtr InternalBusServer_;
    NRpc::IServerPtr InternalRpcServer_;

    TString Fqdn_;
    TAddressMap InternalRpcAddresses_;
    TString ClientGrpcAddress_;
    TString SecureClientGrpcAddress_;
    TString ClientHttpAddress_;
    TString SecureClientHttpAddress_;
    TString AgentGrpcAddress_;

    TInstant ConfigUpdateTime_ = TInstant::Zero();
    TMasterConfigPtr Config_;
    INodePtr ConfigNode_;
    TPeriodicExecutorPtr ConfigUpdateExecutor_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);


    TString BuildGrpcAddress(const NRpc::NGrpc::TServerConfigPtr& config)
    {
        TStringBuf dummyHostName;
        int grpcPort;
        ParseServiceAddress(config->Addresses[0]->Address, &dummyHostName, &grpcPort);
        return BuildServiceAddress(Fqdn_, grpcPort);
    }

    TString BuildHttpAddress(const NYT::NHttp::TServerConfigPtr& config)
    {
        return BuildServiceAddress(Fqdn_, config->Port);
    }

    TString BuildInternalRpcAddress(const NYT::NBus::TTcpBusServerConfigPtr& config)
    {
        return BuildServiceAddress(Fqdn_, *config->Port);
    }

    void DoRun()
    {
        Fqdn_ = GetLocalHostName();
        if (InitialConfig_->InternalBusServer) {
            YT_VERIFY(InternalRpcAddresses_.emplace(DefaultNetworkName, BuildInternalRpcAddress(InitialConfig_->InternalBusServer)).second);
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
        if (InitialConfig_->AgentGrpcServer) {
            AgentGrpcAddress_ = BuildGrpcAddress(InitialConfig_->AgentGrpcServer);
        }

        YT_LOG_INFO("Initializing master (Fqdn: %v)",
            Fqdn_);

        HttpPoller_ = NYT::NConcurrency::CreateThreadPoolPoller(1, "Http");

        YTConnector_ = New<TYTConnector>(Bootstrap_, InitialConfig_->YTConnector);
        ObjectManager_ = New<TObjectManager>(Bootstrap_, InitialConfig_->ObjectManager);
        NetManager_ = New<TNetManager>(Bootstrap_, InitialConfig_->NetManager);
        TransactionManager_ = New<TTransactionManager>(Bootstrap_, InitialConfig_->TransactionManager);
        WatchManager_ = New<TWatchManager>(Bootstrap_, InitialConfig_->WatchManager);
        NodeTracker_ = New<TNodeTracker>(Bootstrap_, InitialConfig_->NodeTracker);
        ResourceManager_ = New<TResourceManager>(Bootstrap_);
        AccessControlManager_ = New<TAccessControlManager>(Bootstrap_, InitialConfig_->AccessControlManager);
        AccountingManager_ = New<TAccountingManager>(Bootstrap_, InitialConfig_->AccountingManager);
        AuthenticationManager_ = New<TAuthenticationManager>(
            InitialConfig_->AuthenticationManager,
            HttpPoller_,
            YTConnector_->GetClient());
        if (InitialConfig_->SecretVaultService && AuthenticationManager_->GetTvmService()) {
            NProfiling::TProfiler secretVaultProfiler("/secret_vault");
            SecretVaultService_ = CreateCachingSecretVaultService(
                InitialConfig_->SecretVaultService,
                CreateBatchingSecretVaultService(
                    InitialConfig_->SecretVaultService,
                    CreateDefaultSecretVaultService(
                        InitialConfig_->SecretVaultService,
                        AuthenticationManager_->GetTvmService(),
                        HttpPoller_,
                        secretVaultProfiler.AppendPath("/remote")),
                    secretVaultProfiler.AppendPath("/batcher")),
                secretVaultProfiler.AppendPath("/cache"));
        } else {
            SecretVaultService_ = CreateDummySecretVaultService();
        }
        Scheduler_ = New<TScheduler>(Bootstrap_, InitialConfig_->Scheduler);

        YTConnector_->Initialize();
        TransactionManager_->Initialize();
        ObjectManager_->Initialize();
        WatchManager_->Initialize();
        AccessControlManager_->Initialize();
        AccountingManager_->Initialize();
        Scheduler_->Initialize();

        if (InitialConfig_->MonitoringServer) {
            HttpMonitoringServer_ = NHttp::CreateServer(
                InitialConfig_->MonitoringServer,
                HttpPoller_);

            HttpMonitoringServer_->AddHandler(
                "/health_check",
                BIND(&TImpl::HealthCheckHandler, this));
        }

        NYTree::IMapNodePtr orchidRoot;
        NMonitoring::Initialize(HttpMonitoringServer_, &MonitoringManager_, &orchidRoot);

        SetNodeByYPath(
            orchidRoot,
            "/access_control",
            CreateVirtualNode(AccessControlManager_->CreateOrchidService()->Via(GetControlInvoker())));
        SetNodeByYPath(
            orchidRoot,
            "/config",
            CreateVirtualNode(CreateConfigOrchidService()->Via(GetControlInvoker())));
        SetNodeByYPath(
            orchidRoot,
            "/initial_config",
            CreateVirtualNode(CreateInitialConfigOrchidService()->Via(GetControlInvoker())));
        SetBuildAttributes(orchidRoot, "yp_master");

        ObjectService_ = NApi::CreateObjectService(Bootstrap_, InitialConfig_->ObjectService);
        ClientDiscoveryService_ = NApi::CreateDiscoveryService(Bootstrap_, EMasterInterface::Client);
        SecureClientDiscoveryService_ = NApi::CreateDiscoveryService(Bootstrap_, EMasterInterface::SecureClient);
        AgentDiscoveryService_ = NApi::CreateDiscoveryService(Bootstrap_, EMasterInterface::Agent);
        NodeTrackerService_ = NNodes::CreateNodeTrackerService(Bootstrap_, InitialConfig_->NodeTracker);

        if (InitialConfig_->InternalBusServer) {
            InternalBusServer_ = NYT::NBus::CreateTcpBusServer(InitialConfig_->InternalBusServer);
        }

        if (InitialConfig_->InternalRpcServer && InternalBusServer_) {
            InternalRpcServer_ = NYT::NRpc::NBus::CreateBusServer(InternalBusServer_);
            InternalRpcServer_->RegisterService(NYT::NOrchid::CreateOrchidService(
                orchidRoot,
                GetControlInvoker()));
            InternalRpcServer_->Configure(InitialConfig_->InternalRpcServer);
        }

        if (InitialConfig_->ClientHttpServer) {
            ClientHttpServer_ = NHttp::CreateServer(
                InitialConfig_->ClientHttpServer,
                HttpPoller_);
            ClientHttpRpcServer_ = NRpc::NHttp::CreateServer(ClientHttpServer_);
            ClientHttpRpcServer_->RegisterService(ObjectService_);
            ClientHttpRpcServer_->RegisterService(ClientDiscoveryService_);
        }

        if (InitialConfig_->SecureClientHttpServer) {
            SecureClientHttpServer_ = NHttps::CreateServer(
                InitialConfig_->SecureClientHttpServer,
                HttpPoller_);
            SecureClientHttpRpcServer_ = NRpc::NHttp::CreateServer(SecureClientHttpServer_);
            SecureClientHttpRpcServer_->RegisterService(ObjectService_);
            SecureClientHttpRpcServer_->RegisterService(SecureClientDiscoveryService_);
        }

        if (InitialConfig_->ClientGrpcServer) {
            ClientGrpcServer_ = NYT::NRpc::NGrpc::CreateServer(InitialConfig_->ClientGrpcServer);
            ClientGrpcServer_->RegisterService(ObjectService_);
            ClientGrpcServer_->RegisterService(ClientDiscoveryService_);
            ClientGrpcServer_->RegisterService(NodeTrackerService_);
        }

        if (InitialConfig_->SecureClientGrpcServer) {
            SecureClientGrpcServer_ = NYT::NRpc::NGrpc::CreateServer(InitialConfig_->SecureClientGrpcServer);
            SecureClientGrpcServer_->RegisterService(ObjectService_);
            SecureClientGrpcServer_->RegisterService(SecureClientDiscoveryService_);
        }

        if (InitialConfig_->AgentGrpcServer) {
            AgentGrpcServer_ = NYT::NRpc::NGrpc::CreateServer(InitialConfig_->AgentGrpcServer);
            AgentGrpcServer_->RegisterService(NodeTrackerService_);
            AgentGrpcServer_->RegisterService(AgentDiscoveryService_);
        }

        YT_LOG_INFO("Listening for incoming connections");

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

        if (AgentGrpcServer_) {
            AgentGrpcServer_->Start();
        }

        StoreConfig(InitialConfig_, InitialConfigNode_);
        ConfigUpdateExecutor_ = New<TPeriodicExecutor>(
            GetControlInvoker(),
            BIND(&TImpl::OnConfigUpdate, this),
            InitialConfig_->ConfigUpdatePeriod);
        ConfigUpdateExecutor_->Start();
    }

    void HealthCheckHandler(
        const NHttp::IRequestPtr& /*req*/,
        const NHttp::IResponseWriterPtr& rsp)
    {
        rsp->SetStatus(YTConnector_->IsConnected()
            ? NHttp::EStatusCode::OK
            : NHttp::EStatusCode::BadRequest);
        WaitFor(rsp->Close())
            .ThrowOnError();
    }

    void StoreConfig(TMasterConfigPtr config, INodePtr configNode)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        ConfigUpdateTime_ = TInstant::Now();
        Config_ = std::move(config);
        ConfigNode_ = std::move(configNode);
        WarnForUnrecognizedOptions(Logger, Config_);
    }

    bool TryStoreConfig(TMasterConfigPtr config)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        auto configNode = ConvertToNode(config);
        if (AreNodesEqual(ConfigNode_, configNode)) {
            return false;
        }
        StoreConfig(std::move(config), std::move(configNode));
        return true;
    }

    void OnConfigUpdateImpl()
    {
        INodePtr cypressConfigPatchNode;
        try {
            auto cypressConfigPatchYsonOrError = WaitFor(YTConnector_->GetClient()->GetNode(YTConnector_->GetMasterPath() + "/config"));
            if (cypressConfigPatchYsonOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                YT_LOG_DEBUG("No configuration patch found in Cypress; trying to update configuration with an empty map");
                cypressConfigPatchNode = GetEphemeralNodeFactory()->CreateMap();
            } else {
                cypressConfigPatchNode = ConvertToNode(cypressConfigPatchYsonOrError.ValueOrThrow());
            }
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error requesting master configuration patch from Cypress")
                << ex;
        }

        TMasterConfigPtr newConfig;
        try {
            // To overcome various corner cases we try to perform update as close as possible
            // to the config load at the program start:
            //  1) Merge data from file and Cypress at yson level.
            //  2) Apply resulting patch to the newly created config object by exactly one Load call.
            auto configPatchNode = PatchNode(InitialConfigPatchNode_, cypressConfigPatchNode);
            newConfig = New<TMasterConfig>();
            newConfig->Load(configPatchNode);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error applying master configuration patch from Cypress")
                << ex;
        }

        if (!TryStoreConfig(newConfig)) {
            YT_LOG_DEBUG("Skipping master configuration update because old and new configurations are equal");
            return;
        }

        YT_LOG_INFO("Applying new master configuration to subsystems");

        // Currently only partial reconfiguration is supported.
        try {
            // Wait for all updates before the exit to prevent concurrent updates.
            // Together with periodic executor serial order it guarantees that
            // bootstrap config will be coherent with internal configs of all components.
            WaitFor(Scheduler_->UpdateConfig(Config_->Scheduler))
                .ThrowOnError();
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Error applying new master configuration")
                << ex;
        }
    }

    void OnConfigUpdate()
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        YT_LOG_DEBUG("Updating master configuration");
        try {
            OnConfigUpdateImpl();
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Error updating master configuration");
        }
    }

    void BuildConfigOrchid(NYson::IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        NYTree::BuildYsonFluently(consumer)
            .BeginAttributes()
                .Item("update_time")
                .Value(ConfigUpdateTime_)
            .EndAttributes()
            .Value(ConfigNode_);
    }

    NYTree::IYPathServicePtr CreateConfigOrchidService()
    {
        auto orchidProducer = BIND(&TImpl::BuildConfigOrchid, this);
        return NYTree::IYPathService::FromProducer(std::move(orchidProducer));
    }

    void BuildInitialConfigOrchid(NYson::IYsonConsumer* consumer)
    {
        VERIFY_THREAD_AFFINITY(ControlThread);

        NYTree::BuildYsonFluently(consumer)
            .Value(InitialConfigNode_);
    }

    NYTree::IYPathServicePtr CreateInitialConfigOrchidService()
    {
        auto orchidProducer = BIND(&TImpl::BuildInitialConfigOrchid, this);
        return NYTree::IYPathService::FromProducer(std::move(orchidProducer));
    }
};

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TMasterConfigPtr config, INodePtr configPatchNode)
    : Impl_(std::make_unique<TImpl>(this, std::move(config), std::move(configPatchNode)))
{ }

const IInvokerPtr& TBootstrap::GetControlInvoker()
{
    return Impl_->GetControlInvoker();
}

const IInvokerPtr& TBootstrap::GetWorkerPoolInvoker()
{
    return Impl_->GetWorkerPoolInvoker();
}

const TYTConnectorPtr& TBootstrap::GetYTConnector()
{
    return Impl_->GetYTConnector();
}

const TObjectManagerPtr& TBootstrap::GetObjectManager()
{
    return Impl_->GetObjectManager();
}

const TNetManagerPtr& TBootstrap::GetNetManager()
{
    return Impl_->GetNetManager();
}

const TTransactionManagerPtr& TBootstrap::GetTransactionManager()
{
    return Impl_->GetTransactionManager();
}

const TNodeTrackerPtr& TBootstrap::GetNodeTracker()
{
    return Impl_->GetNodeTracker();
}

const TWatchManagerPtr& TBootstrap::GetWatchManager()
{
    return Impl_->GetWatchManager();
}

const TSchedulerPtr& TBootstrap::GetScheduler()
{
    return Impl_->GetScheduler();
}

const TResourceManagerPtr& TBootstrap::GetResourceManager()
{
    return Impl_->GetResourceManager();
}

const TAccessControlManagerPtr& TBootstrap::GetAccessControlManager()
{
    return Impl_->GetAccessControlManager();
}

const TAccountingManagerPtr& TBootstrap::GetAccountingManager()
{
    return Impl_->GetAccountingManager();
}

const TAuthenticationManagerPtr& TBootstrap::GetAuthenticationManager()
{
    return Impl_->GetAuthenticationManager();
}

const ISecretVaultServicePtr& TBootstrap::GetSecretVaultService()
{
    return Impl_->GetSecretVaultService();
}

const TString& TBootstrap::GetFqdn()
{
    return Impl_->GetFqdn();
}

const TAddressMap& TBootstrap::GetInternalRpcAddresses()
{
    return Impl_->GetInternalRpcAddresses();
}

const TString& TBootstrap::GetClientGrpcAddress()
{
    return Impl_->GetClientGrpcAddress();
}

const TString& TBootstrap::GetSecureClientGrpcAddress()
{
    return Impl_->GetSecureClientGrpcAddress();
}

const TString& TBootstrap::GetClientHttpAddress()
{
    return Impl_->GetClientHttpAddress();
}

const TString& TBootstrap::GetSecureClientHttpAddress()
{
    return Impl_->GetSecureClientHttpAddress();
}

const TString& TBootstrap::GetAgentGrpcAddress()
{
    return Impl_->GetAgentGrpcAddress();
}

void TBootstrap::Run()
{
    Impl_->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NMaster

