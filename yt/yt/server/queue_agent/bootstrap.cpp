#include "bootstrap.h"

#include "config.h"
#include "private.h"
#include "queue_agent.h"
#include "cypress_synchronizer.h"
#include "dynamic_config_manager.h"
#include "queue_agent_sharding_manager.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/discovery_client/member_client.h>
#include <yt/yt/ytlib/discovery_client/discovery_client.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>
#include <yt/yt/ytlib/hive/cluster_directory.h>

#include <yt/yt/ytlib/queue_client/config.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

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

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ypath/token.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NQueueAgent {

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

static constexpr auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TQueueAgentBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("Control"))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , DynamicConfig_(New<TQueueAgentComponentDynamicConfig>())
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
    const TQueueAgentBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;
    const TQueueAgentComponentDynamicConfigPtr DynamicConfig_;

    TString AgentId_;
    TString GroupId_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NYT::NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    NHiveClient::TClientDirectoryPtr ClientDirectory_;

    NDiscoveryClient::IMemberClientPtr MemberClient_;
    NDiscoveryClient::IDiscoveryClientPtr DiscoveryClient_;
    NCypressElection::ICypressElectionManagerPtr ElectionManager_;

    NAlertManager::IAlertManagerPtr AlertManager_;

    NQueueClient::TDynamicStatePtr DynamicState_;

    IQueueAgentShardingManagerPtr QueueAgentShardingManager_;
    TQueueAgentPtr QueueAgent_;

    ICypressSynchronizerPtr CypressSynchronizer_;

    void DoInitialize()
    {
        YT_LOG_INFO(
            "Starting queue agent process (NativeCluster: %v, User: %v)",
            Config_->ClusterConnection->Static->ClusterName,
            Config_->User);

        AgentId_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);
        GroupId_ = "/queue_agents";

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

        NativeConnection_->GetClusterDirectorySynchronizer()->Start();
        NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(NativeConnection_);

        auto clientOptions = TClientOptions::FromUser(Config_->User);
        NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeClient_);

        DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, NativeClient_, ControlInvoker_);
        DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

        ClientDirectory_ = New<TClientDirectory>(NativeConnection_->GetClusterDirectory(), clientOptions);

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        MemberClient_ = NativeConnection_->CreateMemberClient(
            DynamicConfig_->MemberClient,
            NativeConnection_->GetChannelFactory(),
            ControlInvoker_,
            AgentId_,
            GroupId_);
        DiscoveryClient_ = NativeConnection_->CreateDiscoveryClient(
            DynamicConfig_->DiscoveryClient,
            NativeConnection_->GetChannelFactory());

        {
            TCypressElectionManagerOptionsPtr options = New<TCypressElectionManagerOptions>();
            options->GroupName = "QueueAgent";
            options->MemberName = AgentId_;
            options->TransactionAttributes = CreateEphemeralAttributes();
            options->TransactionAttributes->Set("host", AgentId_);
            ElectionManager_ = CreateCypressElectionManager(NativeClient_, ControlInvoker_, Config_->ElectionManager, std::move(options));
        }

        DynamicState_ = New<TDynamicState>(Config_->DynamicState, NativeClient_, ClientDirectory_);

        AlertManager_ = CreateAlertManager(QueueAgentLogger(), TProfiler{}, ControlInvoker_);

        QueueAgentShardingManager_ = CreateQueueAgentShardingManager(
            ControlInvoker_,
            NativeClient_,
            CreateAlertCollector(AlertManager_),
            DynamicState_,
            MemberClient_,
            DiscoveryClient_,
            Config_->QueueAgent->Stage,
            Config_->DynamicState->Root);

        QueueAgent_ = New<TQueueAgent>(
            Config_->QueueAgent,
            NativeConnection_,
            ClientDirectory_,
            ControlInvoker_,
            DynamicState_,
            ElectionManager_,
            CreateAlertCollector(AlertManager_),
            AgentId_);

        CypressSynchronizer_ = CreateCypressSynchronizer(
            Config_->CypressSynchronizer,
            ControlInvoker_,
            DynamicState_,
            ClientDirectory_,
            CreateAlertCollector(AlertManager_));

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
        SetNodeByYPath(
            orchidRoot,
            "/alerts",
            CreateVirtualNode(AlertManager_->GetOrchidService()));
        SetNodeByYPath(
            orchidRoot,
            "/queue_agent_sharding_manager",
            CreateVirtualNode(QueueAgentShardingManager_->GetOrchidService()));
        SetNodeByYPath(
            orchidRoot,
            "/queue_agent",
            QueueAgent_->GetOrchidNode());
        SetNodeByYPath(
            orchidRoot,
            "/cypress_synchronizer",
            CreateVirtualNode(CypressSynchronizer_->GetOrchidService()));
        SetBuildAttributes(
            orchidRoot,
            "queue_agent");

        RpcServer_->RegisterService(CreateAdminService(
            ControlInvoker_,
            coreDumper,
            NativeAuthenticator_));
        RpcServer_->RegisterService(CreateOrchidService(
            orchidRoot,
            ControlInvoker_,
            NativeAuthenticator_));
    }

    void DoRun()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Configure(Config_->RpcServer);
        RpcServer_->Start();

        UpdateCypressNode();

        YT_UNUSED_FUTURE(MemberClient_->Start());

        ElectionManager_->SubscribeLeadingStarted(BIND_NO_PROPAGATE(&ICypressSynchronizer::Start, CypressSynchronizer_));

        ElectionManager_->SubscribeLeadingEnded(BIND_NO_PROPAGATE(&ICypressSynchronizer::Stop, CypressSynchronizer_));

        ElectionManager_->Start();

        AlertManager_->Start();
        QueueAgentShardingManager_->Start();
        QueueAgent_->Start();
    }

    //! Creates instance node with proper annotations and an orchid node at the native cluster.
    void UpdateCypressNode()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        TCypressRegistrarOptions options{
            .RootPath = Format("%v/instances/%v", Config_->DynamicState->Root, ToYPathLiteral(AgentId_)),
            .OrchidRemoteAddresses = TAddressMap{{NNodeTrackerClient::DefaultNetworkName, AgentId_}},
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
        const TQueueAgentComponentDynamicConfigPtr& oldConfig,
        const TQueueAgentComponentDynamicConfigPtr& newConfig)
    {
        ReconfigureSingletons(newConfig);

        YT_VERIFY(MemberClient_);
        YT_VERIFY(DiscoveryClient_);
        MemberClient_->Reconfigure(newConfig->MemberClient);
        DiscoveryClient_->Reconfigure(newConfig->DiscoveryClient);

        YT_VERIFY(AlertManager_);
        YT_VERIFY(QueueAgentShardingManager_);
        YT_VERIFY(QueueAgent_);
        YT_VERIFY(CypressSynchronizer_);

        std::vector<TFuture<void>> asyncUpdateComponents{
            BIND(
                &IAlertManager::Reconfigure,
                AlertManager_,
                oldConfig->AlertManager,
                newConfig->AlertManager)
                .AsyncVia(ControlInvoker_)
                .Run(),
            BIND(
                &IQueueAgentShardingManager::OnDynamicConfigChanged,
                QueueAgentShardingManager_,
                oldConfig->QueueAgentShardingManager,
                newConfig->QueueAgentShardingManager)
                .AsyncVia(ControlInvoker_)
                .Run(),
            BIND(
                &TQueueAgent::OnDynamicConfigChanged,
                QueueAgent_,
                oldConfig->QueueAgent,
                newConfig->QueueAgent)
                .AsyncVia(ControlInvoker_)
                .Run(),
            BIND(
                &ICypressSynchronizer::OnDynamicConfigChanged,
                CypressSynchronizer_,
                oldConfig->CypressSynchronizer,
                newConfig->CypressSynchronizer)
                .AsyncVia(ControlInvoker_)
                .Run(),
        };
        WaitFor(AllSucceeded(asyncUpdateComponents))
            .ThrowOnError();

        YT_LOG_DEBUG(
            "Updated queue agent server dynamic config (OldConfig: %v, NewConfig: %v)",
            ConvertToYsonString(oldConfig, EYsonFormat::Text),
            ConvertToYsonString(newConfig, EYsonFormat::Text));
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateQueueAgentBootstrap(
    TQueueAgentBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator)
{
    auto bootstrap = New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
    bootstrap->Initialize();
    return bootstrap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
