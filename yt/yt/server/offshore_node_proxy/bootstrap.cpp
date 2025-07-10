#include "bootstrap.h"

#include "config.h"
#include "private.h"
#include "dynamic_config_manager.h"
#include "offshore_node_service.h"

// TODO(achulkov2): Filter unnecessary includes.

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NOffshoreNodeProxy {

using namespace NAdmin;
using namespace NAlertManager;
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
using namespace NSecurityClient;
using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NLogging;
using namespace NCypressElection;
using namespace NHiveClient;
using namespace NYson;
using namespace NQueueClient;
using namespace NFusion;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = OffshoreNodeProxyLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TOffshoreNodeProxyBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("Control"))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , DynamicConfig_(New<TOffshoreNodeProxyDynamicConfig>())
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger(), Config_);
        } else {
            WarnForUnrecognizedOptions(Logger(), Config_);
        }
    }

    TFuture<void> Run() final
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run();
    }

private:
    const TOffshoreNodeProxyBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;
    const TOffshoreNodeProxyDynamicConfigPtr DynamicConfig_;

    TString InstanceId_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NYT::NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        YT_LOG_INFO("Starting offshore node proxy process");

        InstanceId_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

        NApi::NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        NativeConnection_ = NApi::NNative::CreateConnection(
            Config_->ClusterConnection,
            std::move(connectionOptions));

        SetupClusterConnectionDynamicConfigUpdate(
            NativeConnection_,
            Config_->ClusterConnectionDynamicConfigPolicy,
            ConfigNode_->AsMap()->GetChildOrThrow("cluster_connection"),
            Logger());

        NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(NativeConnection_);

        NativeClient_ = NativeConnection_->CreateNativeClient(TClientOptions::FromUser(RootUserName));

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeClient_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, NativeClient_, ControlInvoker_);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        DynamicConfigManager_->Start();

        IMapNodePtr orchidRoot;
        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
            &MonitoringManager_,
            &orchidRoot);

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
            "offshore_node_proxy");

        RpcServer_->RegisterService(CreateAdminService(
            ControlInvoker_,
            coreDumper,
            NativeAuthenticator_));
        RpcServer_->RegisterService(CreateOrchidService(
            orchidRoot,
            ControlInvoker_,
            NativeAuthenticator_));

        RpcServer_->RegisterService(CreateOffshoreNodeService(ControlInvoker_, NativeAuthenticator_));
    }

    void DoStart()
    {
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
            .RootPath = Format("%v/instances/%v", "//sys/offshore_node_proxies", ToYPathLiteral(InstanceId_)),
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
        const TOffshoreNodeProxyDynamicConfigPtr& oldConfig,
        const TOffshoreNodeProxyDynamicConfigPtr& newConfig)
    {
        TSingletonManager::Reconfigure(newConfig);

        YT_LOG_DEBUG(
            "Updated offshore node proxy dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateOffshoreNodeProxyBootstrap(
    TOffshoreNodeProxyBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
