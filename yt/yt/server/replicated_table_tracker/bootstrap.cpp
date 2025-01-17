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

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/library/fusion/service_locator.h>

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
using namespace NFusion;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ReplicatedTableTrackerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TReplicatedTableTrackerBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("RttControl"))
        , ControlInvoker_(ControlQueue_->GetInvoker())
        , RttHostQueue_(New<TActionQueue>("RttHost"))
        , RttHostIvoker_(RttHostQueue_->GetInvoker())
    { }

    TFuture<void> Run() override
    {
        return BIND(&TBootstrap::DoRun, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run();
    }

    const TReplicatedTableTrackerBootstrapConfigPtr& GetServerConfig() const override
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
    const TReplicatedTableTrackerBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;
    const TActionQueuePtr RttHostQueue_;
    const IInvokerPtr RttHostIvoker_;

    std::string LocalAddress_;

    IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;

    ICypressElectionManagerPtr ElectionManager_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    TReplicatedTableTrackerHostPtr ReplicatedTableTrackerHost_;
    IReplicatedTableTrackerPtr ReplicatedTableTracker_;

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
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
        NativeConnection_->GetMasterCellDirectorySynchronizer()->Start();

        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(NativeConnection_);

        NativeClient_ = NativeConnection_->CreateNativeClient(
            NApi::TClientOptions::FromUser(NSecurityClient::RootUserName));

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(NativeClient_);

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        DynamicConfigManager_ = New<TDynamicConfigManager>(
            Config_->DynamicConfigManager,
            NativeClient_,
            ControlInvoker_);
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

        auto coreDumper = ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>();

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
        if (coreDumper) {
            SetNodeByYPath(
                orchidRoot,
                "/core_dumper",
                CreateVirtualNode(coreDumper->CreateOrchidService()));
        }
        SetBuildAttributes(
            orchidRoot,
            "replicated_table_tracker");

        RpcServer_->RegisterService(NAdmin::CreateAdminService(
            ControlInvoker_,
            coreDumper,
            NativeAuthenticator_));
        RpcServer_->RegisterService(NOrchid::CreateOrchidService(
            orchidRoot,
            ControlInvoker_,
            NativeAuthenticator_));
    }

    void DoStart()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Configure(Config_->RpcServer);
        RpcServer_->Start();

        ReplicatedTableTrackerHost_ = New<TReplicatedTableTrackerHost>(this);
        ReplicatedTableTracker_ = CreateReplicatedTableTracker(
            ReplicatedTableTrackerHost_,
            DynamicConfigManager_->GetConfig(),
            ReplicatedTableTrackerProfiler());

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

        // Cycles are fine for bootstrap.
        ElectionManager_->SubscribeLeadingStarted(BIND_NO_PROPAGATE(&TBootstrap::OnLeadingStarted, MakeStrong(this)));
        ElectionManager_->SubscribeLeadingEnded(BIND_NO_PROPAGATE(&TBootstrap::OnLeadingEnded, MakeStrong(this)));

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
        YT_LOG_DEBUG("RTT instance started leading");

        ReplicatedTableTrackerHost_->EnableUpdates();
        ReplicatedTableTracker_->Initialize();
        ReplicatedTableTracker_->EnableTracking();
    }

    void OnLeadingEnded() const
    {
        YT_LOG_DEBUG("RTT instance finished leading");

        ReplicatedTableTrackerHost_->DisableUpdates();
        ReplicatedTableTracker_->DisableTracking();
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateReplicatedTableTrackerBootstrap(
    TReplicatedTableTrackerBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTracker
