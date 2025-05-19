#include "bootstrap.h"

#include "query_tracker.h"
#include "query_tracker_proxy.h"
#include "config.h"
#include "private.h"
#include "proxy_service.h"
#include "dynamic_config_manager.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/state_checker/state_checker.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NQueryTracker {

using namespace NAdmin;
using namespace NAlertManager;
using namespace NBus;
using namespace NHydra;
using namespace NMonitoring;
using namespace NObjectClient;
using namespace NChunkClient;
using namespace NOrchid;
using namespace NComponentStateChecker;
using namespace NProfiling;
using namespace NRpc;
using namespace NTransactionClient;
using namespace NSecurityClient;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NLogging;
using namespace NHiveClient;
using namespace NYson;
using namespace NTableClient;
using namespace NFusion;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = QueryTrackerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TQueryTrackerBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("Control"))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , ProxyPool_(CreateThreadPool(Config_->ProxyThreadPoolSize, "Proxy"))
        , ProxyInvoker_(ProxyPool_->GetInvoker())
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }
    }

    void Initialize()
    {
        BIND(&TBootstrap::DoInitialize, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run()
            .Get()
            .ThrowOnError();
    }

    TFuture<void> Run() final
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run();
    }

private:
    const TQueryTrackerBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;
    const NConcurrency::IThreadPoolPtr ProxyPool_;
    const IInvokerPtr ProxyInvoker_;

    std::string SelfAddress_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NYT::NBus::IBusServerPtr BusServer_;
    IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NComponentStateChecker::IComponentStateCheckerPtr ComponentStateChecker_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    NAlertManager::IAlertManagerPtr AlertManager_;

    IQueryTrackerPtr QueryTracker_;

    TQueryTrackerProxyPtr QueryTrackerProxy_;

    void DoInitialize()
    {
        YT_LOG_INFO(
            "Starting persistent query agent process (NativeCluster: %v, User: %v)",
            Config_->ClusterConnection->Static->ClusterName,
            Config_->User);

        SelfAddress_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

        NApi::NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        NativeConnection_ = NApi::NNative::CreateConnection(
            Config_->ClusterConnection,
            std::move(connectionOptions));

        NativeConnection_->GetClusterDirectorySynchronizer()->Start();
        NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();

        SetupClusterConnectionDynamicConfigUpdate(
            NativeConnection_,
            NApi::NNative::EClusterConnectionDynamicConfigPolicy::FromClusterDirectory,
            /*staticClusterConnectionNode*/ nullptr,
            Logger());

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(NativeConnection_);

        auto clientOptions = TClientOptions::FromUser(Config_->User);
        NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeClient_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, NativeClient_, ControlInvoker_);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        AlertManager_ = CreateAlertManager(QueryTrackerLogger(), TProfiler{}, ControlInvoker_);

        DynamicConfigManager_->Start();

        {
            YT_LOG_INFO("Loading dynamic config for the first time");
            auto error = WaitFor(DynamicConfigManager_->GetConfigLoadedFuture());
            YT_LOG_FATAL_UNLESS(
                error.IsOK(),
                error,
                "Unexpected failure while waiting for the first dynamic config loaded");
            YT_LOG_INFO("Dynamic config loaded");
        }

        IMapNodePtr orchidRoot;
        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
            &MonitoringManager_,
            &orchidRoot);

        SetNodeByYPath(
            orchidRoot,
            "/alerts",
            CreateVirtualNode(AlertManager_->GetOrchidService()));
        if (Config_->ExposeConfigInOrchid) {
            SetNodeByYPath(
                orchidRoot,
                "/config",
                CreateVirtualNode(ConfigNode_));
            SetNodeByYPath(
                orchidRoot,
                "/dynamic_config_manager",
                CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        }
        auto coreDumper = ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>();
        if (coreDumper) {
            SetNodeByYPath(
                orchidRoot,
                "/core_dumper",
                CreateVirtualNode(coreDumper->CreateOrchidService()));
        }
        SetBuildAttributes(
            orchidRoot,
            "query_tracker");

        QueryTrackerProxy_ = CreateQueryTrackerProxy(
            NativeClient_,
            Config_->Root,
            DynamicConfigManager_->GetConfig()->QueryTracker->ProxyConfig);

        ComponentStateChecker_ = CreateComponentStateChecker(
            ControlInvoker_,
            NativeClient_,
            Format("%v/instances/%v", Config_->Root, ToYPathLiteral(SelfAddress_)),
            DynamicConfigManager_->GetConfig()->QueryTracker->StateCheckPeriod);
        SetNodeByYPath(
            orchidRoot,
            "/state_checker",
            CreateVirtualNode(ComponentStateChecker_->GetOrchidService()));

        RpcServer_->RegisterService(CreateAdminService(
            ControlInvoker_,
            coreDumper,
            NativeAuthenticator_));
        RpcServer_->RegisterService(CreateOrchidService(
            orchidRoot,
            ControlInvoker_,
            NativeAuthenticator_));
        RpcServer_->RegisterService(CreateProxyService(
            ProxyInvoker_,
            QueryTrackerProxy_,
            ComponentStateChecker_));

        QueryTracker_ = CreateQueryTracker(
            DynamicConfigManager_->GetConfig()->QueryTracker,
            SelfAddress_,
            ControlInvoker_,
            CreateAlertCollector(AlertManager_),
            NativeClient_,
            ComponentStateChecker_,
            Config_->Root,
            Config_->MinRequiredStateVersion);
        SetNodeByYPath(
            orchidRoot,
            "/query_tracker",
            CreateVirtualNode(QueryTracker_->GetOrchidService()));
    }

    void DoRun()
    {
        AlertManager_->Start();

        QueryTracker_->Start();

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Configure(Config_->RpcServer);
        RpcServer_->Start();

        UpdateCypressNode();
        ComponentStateChecker_->Start();
    }

    //! Creates instance node with proper annotations and an orchid node at the native cluster.
    void UpdateCypressNode()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        TCypressRegistrarOptions options{
            .RootPath = Format("%v/instances/%v", Config_->Root, ToYPathLiteral(SelfAddress_)),
            .OrchidRemoteAddresses = TAddressMap{{NNodeTrackerClient::DefaultNetworkName, SelfAddress_}},
            .AttributesOnStart = BuildAttributeDictionaryFluently()
                .Item("annotations").Value(Config_->CypressAnnotations)
                .Finish(),
        };

        auto registrar = CreateCypressRegistrar(
            std::move(options),
            New<TCypressRegistrarConfig>(),
            NativeClient_,
            GetCurrentInvoker());

        while (true) {
            auto error = WaitFor(registrar->CreateNodes());

            if (error.IsOK()) {
                break;
            } else {
                YT_LOG_DEBUG(error, "Error updating Cypress node");
            }
        }
    }

    void OnDynamicConfigChanged(
        const TQueryTrackerComponentDynamicConfigPtr& oldConfig,
        const TQueryTrackerComponentDynamicConfigPtr& newConfig)
    {
        TSingletonManager::Reconfigure(newConfig);

        if (AlertManager_) {
            AlertManager_->Reconfigure(oldConfig->AlertManager, newConfig->AlertManager);
        }
        if (QueryTracker_) {
            QueryTracker_->Reconfigure(newConfig->QueryTracker);
        }

        if (ComponentStateChecker_) {
            ComponentStateChecker_->SetPeriod(newConfig->QueryTracker->StateCheckPeriod);
        }

        if (QueryTrackerProxy_) {
            QueryTrackerProxy_->Reconfigure(newConfig->QueryTracker->ProxyConfig);
        }

        YT_LOG_DEBUG(
            "Updated query tracker server dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }

    void CreateStateTablesIfNeeded();
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateQueryTrackerBootstrap(
    TQueryTrackerBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    auto bootstrap = New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
    bootstrap->Initialize();
    return bootstrap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
