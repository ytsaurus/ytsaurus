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
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/library/coredumper/coredumper.h>
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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueueAgentLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TQueueAgentServerConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , DynamicConfig_(New<TQueueAgentServerDynamicConfig>())
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    ControlQueue_ = New<TActionQueue>("Control");
    ControlInvoker_ = ControlQueue_->GetInvoker();

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(ControlInvoker_)
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
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

    NativeConnection_->GetClusterDirectorySynchronizer()->Start();

    NativeAuthenticator_ = NNative::CreateNativeAuthenticator(NativeConnection_);

    auto clientOptions = TClientOptions::FromUser(Config_->User);
    NativeClient_ = NativeConnection_->CreateNativeClient(clientOptions);

    DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, NativeClient_, ControlInvoker_);
    DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, Unretained(this)));

    ClientDirectory_ = New<TClientDirectory>(NativeConnection_->GetClusterDirectory(), clientOptions);

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

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

    AlertManager_ = CreateAlertManager(ControlInvoker_);

    QueueAgentShardingManager_ = CreateQueueAgentShardingManager(
        ControlInvoker_,
        DynamicState_,
        MemberClient_,
        DiscoveryClient_,
        Config_->QueueAgent->Stage);

    QueueAgent_ = New<TQueueAgent>(
        Config_->QueueAgent,
        NativeConnection_,
        ClientDirectory_,
        ControlInvoker_,
        DynamicState_,
        ElectionManager_,
        AgentId_);

    CypressSynchronizer_ = CreateCypressSynchronizer(
        Config_->CypressSynchronizer,
        ControlInvoker_,
        DynamicState_,
        ClientDirectory_);

    DynamicConfigManager_->Start();

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);

    SetNodeByYPath(
        orchidRoot,
        "/config",
        CreateVirtualNode(ConfigNode_));
    SetNodeByYPath(
        orchidRoot,
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
    if (CoreDumper_) {
        SetNodeByYPath(
            orchidRoot,
            "/core_dumper",
            CreateVirtualNode(CoreDumper_->CreateOrchidService()));
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
        CoreDumper_,
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        ControlInvoker_,
        NativeAuthenticator_));

    YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();

    UpdateCypressNode();

    YT_UNUSED_FUTURE(MemberClient_->Start());

    AlertManager_->SubscribePopulateAlerts(BIND(&IQueueAgentShardingManager::PopulateAlerts, QueueAgentShardingManager_));
    AlertManager_->SubscribePopulateAlerts(BIND(&TQueueAgent::PopulateAlerts, QueueAgent_));
    AlertManager_->SubscribePopulateAlerts(BIND(&ICypressSynchronizer::PopulateAlerts, CypressSynchronizer_));

    ElectionManager_->SubscribeLeadingStarted(BIND(&ICypressSynchronizer::Start, CypressSynchronizer_));

    ElectionManager_->SubscribeLeadingEnded(BIND(&ICypressSynchronizer::Stop, CypressSynchronizer_));

    ElectionManager_->Start();

    AlertManager_->Start();
    QueueAgentShardingManager_->Start();
    QueueAgent_->Start();
}

void TBootstrap::UpdateCypressNode()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

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

void TBootstrap::OnDynamicConfigChanged(
    const TQueueAgentServerDynamicConfigPtr& oldConfig,
    const TQueueAgentServerDynamicConfigPtr& newConfig)
{
    ReconfigureNativeSingletons(Config_, newConfig);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
