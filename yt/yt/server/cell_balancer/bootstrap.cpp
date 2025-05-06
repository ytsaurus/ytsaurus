#include "bootstrap.h"

#include "bundle_controller.h"
#include "bundle_controller_service.h"
#include "cell_tracker.h"
#include "config.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>
#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NCellBalancer {

using namespace NAdmin;
using namespace NApi;
using namespace NConcurrency;
using namespace NCoreDump;
using namespace NCypressElection;
using namespace NFusion;
using namespace NMonitoring;
using namespace NNodeTrackerClient;
using namespace NOrchid;
using namespace NTransactionClient;
using namespace NYPath;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CellBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(
        TCellBalancerBootstrapConfigPtr config,
        INodePtr configNode,
        IServiceLocatorPtr serviceLocator)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
        , ServiceLocator_(std::move(serviceLocator))
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
            .AsyncVia(GetControlInvoker())
            .Run();
    }

    const NApi::NNative::IClientPtr& GetClient() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return Client_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ControlQueue_->GetInvoker();
    }

    TAddressMap GetLocalAddresses() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NServer::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);
    }

    const ICypressElectionManagerPtr& GetElectionManager() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ElectionManager_;
    }


    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NativeAuthenticator_;
    }

    void ExecuteDryRunIteration() override
    {
        DoInitialize();

        YT_LOG_DEBUG("Dry run iteration started");

        WaitFor(
            BIND(&IBundleController::ExecuteDryRunIteration, BundleController_)
                .AsyncVia(GetControlInvoker())
                .Run())
            .ThrowOnError();

        YT_LOG_DEBUG("Dry run iteration finished");
    }

private:
    const TCellBalancerBootstrapConfigPtr Config_;
    const INodePtr ConfigNode_;
    const IServiceLocatorPtr ServiceLocator_;

    const TActionQueuePtr ControlQueue_ = New<TActionQueue>("Control");

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    IMonitoringManagerPtr MonitoringManager_;

    NNative::IConnectionPtr Connection_;
    NNative::IClientPtr Client_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    ICypressElectionManagerPtr ElectionManager_;
    ICellTrackerPtr CellTracker_;
    IBundleControllerPtr BundleController_;

    void DoRun()
    {
        DoInitialize();
        DoStart();
    }

    void DoInitialize()
    {
        NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        Connection_ = NNative::CreateConnection(Config_->ClusterConnection, connectionOptions);
        Connection_->GetClusterDirectorySynchronizer()->Start();

        auto clientOptions = TClientOptions::FromUser(NSecurityClient::RootUserName);
        Client_ = Connection_->CreateNativeClient(clientOptions);

        NLogging::GetDynamicTableLogWriterFactory()->SetClient(Client_);

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(Connection_);

        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        TCypressElectionManagerOptionsPtr options = New<TCypressElectionManagerOptions>();
        options->GroupName = "CellBalancer";
        options->MemberName = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);
        ElectionManager_ = CreateCypressElectionManager(
            Client_,
            GetControlInvoker(),
            Config_->ElectionManager,
            options);

        CellTracker_ = CreateCellTracker(this, Config_->CellBalancer);
        BundleController_ = CreateBundleController(this, Config_->BundleController);

        NMonitoring::Initialize(
            HttpServer_,
            ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
            &MonitoringManager_,
            &OrchidRoot_);

        if (Config_->ExposeConfigInOrchid) {
            SetNodeByYPath(
                OrchidRoot_,
                "/config",
                CreateVirtualNode(ConfigNode_));
        }
        SetNodeByYPath(
            OrchidRoot_,
            "/cell_balancer",
            CreateVirtualNode(CellTracker_->CreateOrchidService()->Via(GetControlInvoker())));
        SetBuildAttributes(
            OrchidRoot_,
            "cell_balancer");
        SetNodeByYPath(
            OrchidRoot_,
            "/bundle_controller",
            CreateVirtualNode(BundleController_->CreateOrchidService()->Via(GetControlInvoker())));

        RpcServer_->RegisterService(CreateOrchidService(
            OrchidRoot_,
            GetControlInvoker(),
            NativeAuthenticator_));
        RpcServer_->RegisterService(CreateAdminService(
            GetControlInvoker(),
            ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
            NativeAuthenticator_));
        RpcServer_->RegisterService(NBundleController::CreateBundleControllerService(this));
    }

    void DoStart()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();

        RegisterInstance();

        ElectionManager_->Start();

        if (Config_->EnableCellBalancer) {
            CellTracker_->Start();
        }

        if (Config_->EnableBundleController) {
            BundleController_->Start();
        }

        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);
    }

    void RegisterInstance()
    {
        TCypressRegistrarOptions options{
            .RootPath = Format(
                "//sys/cell_balancers/instances/%v",
                ToYPathLiteral(GetDefaultAddress(GetLocalAddresses()))),
            .OrchidRemoteAddresses = GetLocalAddresses(),
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

IBootstrapPtr CreateCellBalancerBootstrap(
    TCellBalancerBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
