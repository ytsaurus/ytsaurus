#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "tablet_balancer.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/cypress_registrar/config.h>
#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory.h>
#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/node.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressElection;
using namespace NHiveClient;
using namespace NNodeTrackerClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NFusion;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TTabletBalancerBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
        , ControlQueue_(New<TActionQueue>("Control"))
        , ControlInvoker_(ControlQueue_->GetInvoker())
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
            .AsyncVia(ControlQueue_->GetInvoker())
            .Run();
    }

    const NNative::IClientPtr& GetClient() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Client_;
    }

    const TClientDirectoryPtr& GetClientDirectory() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ClientDirectory_;
    }

    const ICypressElectionManagerPtr& GetElectionManager() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ElectionManager_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ControlInvoker_;
    }

    const TDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return DynamicConfigManager_;
    }

    std::string GetClusterName() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        YT_VERIFY(Config_);

        return Config_->ClusterConnection->Static->ClusterName.value();
    }

private:
    const TTabletBalancerBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_;
    const IInvokerPtr ControlInvoker_;

    ITabletBalancerPtr TabletBalancer_;

    std::string LocalAddress_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    NNative::IConnectionPtr Connection_;
    NNative::IClientPtr Client_;
    NHiveClient::TClientDirectoryPtr ClientDirectory_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    ICypressElectionManagerPtr ElectionManager_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        YT_LOG_INFO("Starting tablet balancer process (ClusterName: %v)",
            Config_->ClusterConnection->Static->ClusterName);

        LocalAddress_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

        NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        Connection_ = NNative::CreateConnection(
            Config_->ClusterConnection,
            connectionOptions);

        SetupClusterConnectionDynamicConfigUpdate(
            Connection_,
            Config_->ClusterConnectionDynamicConfigPolicy,
            ConfigNode_->AsMap()->GetChildOrThrow("cluster_connection"),
            Logger());

        Connection_->GetClusterDirectorySynchronizer()->Start();
        Connection_->GetMasterCellDirectorySynchronizer()->Start();

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(Connection_);

        auto clientOptions = TClientOptions::FromUser(Config_->ClusterUser);
        Client_ = Connection_->CreateNativeClient(clientOptions);

        ClientDirectory_ = New<TClientDirectory>(Connection_->GetClusterDirectory(), clientOptions);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(Client_);

        BusServer_ = CreateBusServer(Config_->BusServer);

        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, this);
        DynamicConfigManager_->Start();

        TabletBalancer_ = CreateTabletBalancer(
            this,
            Config_->TabletBalancer,
            ControlInvoker_);

        IMapNodePtr orchidRoot;
        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
            &MonitoringManager_,
            &orchidRoot);

        auto coreDumper = ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>();
        if (coreDumper) {
            SetNodeByYPath(
                orchidRoot,
                "/core_dumper",
                CreateVirtualNode(coreDumper->CreateOrchidService()));
        }

        SetNodeByYPath(
            orchidRoot,
            "/tablet_balancer",
            CreateVirtualNode(TabletBalancer_->GetOrchidService()));

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
        SetBuildAttributes(
            orchidRoot,
            "tablet_balancer");

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

        RegisterInstance();

        TCypressElectionManagerOptionsPtr options = New<TCypressElectionManagerOptions>();
        options->GroupName = "TabletBalancer";
        options->MemberName = LocalAddress_;
        options->TransactionAttributes = CreateEphemeralAttributes();
        options->TransactionAttributes->Set("host", LocalAddress_);

        ElectionManager_ = CreateCypressElectionManager(
            Client_,
            ControlInvoker_,
            Config_->ElectionManager,
            options);

        ElectionManager_->SubscribeLeadingStarted(BIND(&ITabletBalancer::Start, TabletBalancer_));
        ElectionManager_->SubscribeLeadingEnded(BIND(&ITabletBalancer::Stop, TabletBalancer_));

        ElectionManager_->Start();

        YT_LOG_INFO("Finished initializing bootstrap");
    }

    void RegisterInstance()
    {
        TCypressRegistrarOptions options{
            .RootPath = Format("%v/instances/%v", Config_->RootPath, ToYPathLiteral(LocalAddress_)),
            .OrchidRemoteAddresses = TAddressMap{{NNodeTrackerClient::DefaultNetworkName, LocalAddress_}},
        };

        auto registrar = CreateCypressRegistrar(
            std::move(options),
            New<TCypressRegistrarConfig>(),
            Client_,
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
};

////////////////////////////////////////////////////////////////////////////////

IBootstrapPtr CreateTabletBalancerBootstrap(
    TTabletBalancerBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
