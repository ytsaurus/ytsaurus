#include "bootstrap.h"

#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "replicated_table_tracker_host.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>
#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/server/lib/tablet_server/replicated_table_tracker.h>
#include <yt/yt/server/lib/tablet_server/config.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/auth/native_authenticator.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NReplicatedTableTracker {

using namespace NApi::NNative;
using namespace NConcurrency;
using namespace NCypressElection;
using namespace NHttp;
using namespace NTabletServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ReplicatedTableTrackerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(TReplicatedTableTrackerServerConfigPtr config, INodePtr configNode)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
    { }

    void Run() override
    {
        ControlQueue_ = New<TActionQueue>("RttControl");
        ControlInvoker_ = ControlQueue_->GetInvoker();

        BIND(&TBootstrap::DoRun, this)
            .AsyncVia(ControlInvoker_)
            .Run()
            .Get()
            .ThrowOnError();

        Sleep(TDuration::Max());
    }

    const TReplicatedTableTrackerServerConfigPtr& GetServerConfig() const override
    {
        return Config_;
    }

    const TDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        return DynamicConfigManager_;
    }

    const IConnectionPtr& GetClusterConnection() const override
    {
        return NativeConnection_;
    }

    const IInvokerPtr& GetRttHostInvoker() const override
    {
        return RttHostIvoker_;
    }

private:
    const TReplicatedTableTrackerServerConfigPtr Config_;
    const INodePtr ConfigNode_;

    TActionQueuePtr ControlQueue_;
    IInvokerPtr ControlInvoker_;

    TActionQueuePtr RttHostQueue_;
    IInvokerPtr RttHostIvoker_;

    TString LocalAddress_;

    IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    NCoreDump::ICoreDumperPtr CoreDumper_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;

    ICypressElectionManagerPtr ElectionManager_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    TReplicatedTableTrackerHostPtr ReplicatedTableTrackerHost_;
    IReplicatedTableTrackerPtr ReplicatedTableTracker_;


    void DoRun()
    {
        YT_LOG_INFO("Starting replicated table tracker process (ClusterName: %v)",
            Config_->ClusterConnection->Static->ClusterName);

        LocalAddress_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

        TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        NativeConnection_ = CreateConnection(
            Config_->ClusterConnection,
            std::move(connectionOptions));

        NativeConnection_->GetClusterDirectorySynchronizer()->Start();

        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

        NativeClient_ = NativeConnection_->CreateNativeClient(
            NApi::TClientOptions::FromUser(NSecurityClient::RootUserName));

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        if (Config_->CoreDumper) {
            CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
        }

        DynamicConfigManager_ = New<TDynamicConfigManager>(
            Config_->DynamicConfigManager,
            NativeClient_,
            ControlInvoker_);
        DynamicConfigManager_->Start();
        WaitFor(DynamicConfigManager_->GetConfigLoadedFuture())
            .ThrowOnError();

        IMapNodePtr orchidRoot;
        Initialize(
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

        RpcServer_->RegisterService(NAdmin::CreateAdminService(
            ControlInvoker_,
            CoreDumper_,
            NativeAuthenticator_));
        RpcServer_->RegisterService(NOrchid::CreateOrchidService(
            orchidRoot,
            ControlInvoker_,
            NativeAuthenticator_));

        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Configure(Config_->RpcServer);
        RpcServer_->Start();

        RttHostQueue_ = New<TActionQueue>("RttHost");
        RttHostIvoker_ = RttHostQueue_->GetInvoker();

        ReplicatedTableTrackerHost_ = New<TReplicatedTableTrackerHost>(this);
        ReplicatedTableTracker_ = CreateReplicatedTableTracker(
            ReplicatedTableTrackerHost_,
            DynamicConfigManager_->GetConfig(),
            ReplicatedTableTrackerProfiler);

        RegisterInstance();

        TCypressElectionManagerOptionsPtr options = New<TCypressElectionManagerOptions>();
        options->GroupName = "ReplicatedTableTracker";
        options->MemberName = LocalAddress_;
        options->TransactionAttributes = CreateEphemeralAttributes();
        options->TransactionAttributes->Set("host", LocalAddress_);

        ElectionManager_ = CreateCypressElectionManager(
            NativeClient_,
            ControlInvoker_,
            Config_->ElectionManager,
            std::move(options));

        ElectionManager_->SubscribeLeadingStarted(BIND(&TBootstrap::OnLeadingStarted, this));
        ElectionManager_->SubscribeLeadingEnded(BIND(&TBootstrap::OnLeadingEnded, this));

        ElectionManager_->Start();

        YT_LOG_INFO("Finished initializing bootstrap");
    }

    void RegisterInstance()
    {
        TCypressRegistrarOptions options{
            .RootPath = Format("//sys/replicated_table_tracker/instances/%v", NYPath::ToYPathLiteral(LocalAddress_)),
            .OrchidRemoteAddresses = NNodeTrackerClient::TAddressMap{{NNodeTrackerClient::DefaultNetworkName, LocalAddress_}},
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

    void OnLeadingStarted() const
    {
        YT_LOG_DEBUG("Rtt instance started leading");

        ReplicatedTableTrackerHost_->EnableUpdates();
        ReplicatedTableTracker_->Initialize();
        ReplicatedTableTracker_->EnableTracking();
    }

    void OnLeadingEnded() const
    {
        YT_LOG_DEBUG("Rtt instance finished leading");

        ReplicatedTableTrackerHost_->DisableUpdates();
        ReplicatedTableTracker_->DisableTracking();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(
    TReplicatedTableTrackerServerConfigPtr config,
    NYTree::INodePtr configNode)
{
    return std::make_unique<TBootstrap>(std::move(config), std::move(configNode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
