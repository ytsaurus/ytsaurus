#include "bootstrap.h"

#include "private.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "offshore_data_gateway_service.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/api/rpc_proxy/address_helpers.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/bus/tcp/server.h>
#include <yt/yt/core/bus/tcp/dispatcher.h>

#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/library/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NOffshoreDataGateway {

using namespace NAdmin;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NApi;
using namespace NYson;
using namespace NFusion;
using namespace NNodeTrackerClient;
using namespace NOrchid;
using namespace NProfiling;
using namespace NAlertManager;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = OffshoreDataGatewayLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TOffshoreDataGatewayBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("Control"))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , DynamicConfig_(New<TOffshoreDataGatewayDynamicConfig>())
        , StorageThreadPool_(CreateThreadPool(
            DynamicConfig_->StorageThreadCount,
            /*threadNamePrefix*/ "Storage"))
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }
    }

    TFuture<void> Run() override
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run();
    }

private:
    const TOffshoreDataGatewayBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;
    const TOffshoreDataGatewayDynamicConfigPtr DynamicConfig_;

    const IThreadPoolPtr StorageThreadPool_;

    std::string InstanceId_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NNative::IConnectionPtr NativeConnection_;
    NNative::IClientPtr NativeClient_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    NAlertManager::IAlertManagerPtr AlertManager_;

    void DoRun()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        InstanceId_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);
        YT_LOG_INFO("Starting offshore data gateway process (InstanceId: %v)", InstanceId_);

        NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        NativeConnection_ = NNative::CreateConnection(
            Config_->ClusterConnection,
            std::move(connectionOptions));

        SetupClusterConnectionDynamicConfigUpdate(
            NativeConnection_,
            Config_->ClusterConnectionDynamicConfigPolicy,
            ConfigNode_->AsMap()->GetChildOrThrow("cluster_connection"),
            Logger());

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(NativeConnection_);

        auto clientOptions = NNative::TClientOptions::FromUser(NRpc::RootUserName);
        NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeClient_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, NativeClient_, ControlInvoker_);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));
        DynamicConfigManager_->Start();

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        AlertManager_ = CreateAlertManager(OffshoreDataGatewayLogger(), TProfiler{}, ControlInvoker_);

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
            "offshore_data_gateway");

        RpcServer_->RegisterService(CreateAdminService(
            ControlInvoker_,
            coreDumper,
            NativeAuthenticator_));
        RpcServer_->RegisterService(CreateOrchidService(
            orchidRoot,
            ControlInvoker_,
            NativeAuthenticator_));

        RpcServer_->RegisterService(CreateOffshoreDataGatewayService(
            ControlInvoker_,
            StorageThreadPool_->GetInvoker(),
            NBus::TTcpDispatcher::Get()->GetXferPoller(),
            NativeAuthenticator_,
            NativeConnection_->GetMediumDirectory(),
            NativeConnection_->GetMediumDirectorySynchronizer()));
    }

    void DoStart()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        NativeConnection_->GetClusterDirectorySynchronizer()->Start();
        NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();
        NativeConnection_->GetMediumDirectorySynchronizer()->Start();

        AlertManager_->Start();

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Configure(Config_->RpcServer);
        RpcServer_->Start();

        UpdateCypressNode();
    }

    void UpdateCypressNode()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        TCypressRegistrarOptions options{
            .RootPath = Format("%v/instances/%v", "//sys/offshore_data_gateways", ToYPathLiteral(InstanceId_)),
            .OrchidRemoteAddresses = TAddressMap{{NNodeTrackerClient::DefaultNetworkName, InstanceId_}},
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
        const TOffshoreDataGatewayDynamicConfigPtr& oldConfig,
        const TOffshoreDataGatewayDynamicConfigPtr& newConfig)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        TSingletonManager::Reconfigure(newConfig);

        StorageThreadPool_->SetThreadCount(newConfig->StorageThreadCount);

        YT_LOG_DEBUG(
            "Updated offshore data gateway dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateOffshoreDataGatewayBootstrap(
    TOffshoreDataGatewayBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
