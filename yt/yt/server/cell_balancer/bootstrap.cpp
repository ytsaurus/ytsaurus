#include "bootstrap.h"

#include "cell_tracker.h"
#include "config.h"
#include "master_connector.h"
#include "bundle_controller.h"
#include "bundle_controller_service.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/library/coredumper/public.h>

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
using namespace NNodeTrackerClient;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NTransactionClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    explicit TBootstrap(TCellBalancerBootstrapConfigPtr config)
        : Config_(std::move(config))
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger, Config_);
        } else {
            WarnForUnrecognizedOptions(Logger, Config_);
        }
    }

    void Initialize() override
    {
        ControlQueue_ = New<TActionQueue>("Control");

        BIND(&TBootstrap::DoInitialize, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
    }

    void Run() override
    {
        BIND(&TBootstrap::DoRun, this)
            .AsyncVia(GetControlInvoker())
            .Run()
            .Get()
            .ThrowOnError();
        Sleep(TDuration::Max());
    }

    const NApi::NNative::IClientPtr& GetClient() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Client_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ControlQueue_->GetInvoker();
    }

    TAddressMap GetLocalAddresses() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);
    }

    const ICypressElectionManagerPtr& GetElectionManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ElectionManager_;
    }


    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return NativeAuthenticator_;
    }

private:
    const TCellBalancerBootstrapConfigPtr Config_;

    TActionQueuePtr ControlQueue_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;

    IMapNodePtr OrchidRoot_;
    TMonitoringManagerPtr MonitoringManager_;

    NCoreDump::ICoreDumperPtr CoreDumper_;

    NNative::IConnectionPtr Connection_;
    NNative::IClientPtr Client_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    ICypressElectionManagerPtr ElectionManager_;
    IMasterConnectorPtr MasterConnector_;
    ICellTrackerPtr CellTracker_;
    IBundleControllerPtr BundleController_;

    void DoInitialize()
    {
        NNative::TConnectionOptions connectionOptions;
        connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
        Connection_ = NNative::CreateConnection(Config_->ClusterConnection, connectionOptions);

        auto clientOptions = TClientOptions::FromUser(NSecurityClient::RootUserName);
        Client_ = Connection_->CreateNativeClient(clientOptions);

        NativeAuthenticator_ = NNative::CreateNativeAuthenticator(Connection_);

        BusServer_ = NBus::CreateBusServer(Config_->BusServer);
        RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);
        HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

        if (Config_->CoreDumper) {
            CoreDumper_ = CreateCoreDumper(Config_->CoreDumper);
        }

        TCypressElectionManagerOptionsPtr options = New<TCypressElectionManagerOptions>();
        options->GroupName = "CellBalancer";
        options->MemberName = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);
        ElectionManager_ = CreateCypressElectionManager(
            Client_,
            GetControlInvoker(),
            Config_->ElectionManager,
            options);

        MasterConnector_ = CreateMasterConnector(this, Config_->MasterConnector);
        CellTracker_ = CreateCellTracker(this, Config_->CellBalancer);
        BundleController_ = CreateBundleController(this, Config_->BundleController);

        NMonitoring::Initialize(
            HttpServer_,
            Config_->SolomonExporter,
            &MonitoringManager_,
            &OrchidRoot_);

        SetNodeByYPath(
            OrchidRoot_,
            "/config",
            CreateVirtualNode(ConvertTo<INodePtr>(Config_)));
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
            CoreDumper_,
            NativeAuthenticator_));
        RpcServer_->RegisterService(NBundleController::CreateBundleControllerService(this));
    }

    void DoRun()
    {
        YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
        HttpServer_->Start();

        YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
        RpcServer_->Start();

        MasterConnector_->Start();

        ElectionManager_->Start();

        if (Config_->EnableCellBalancer) {
            CellTracker_->Start();
        }

        if (Config_->EnableBundleController) {
            BundleController_->Start();
        }

        NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TCellBalancerBootstrapConfigPtr config)
{
    return std::make_unique<TBootstrap>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
