#include "bootstrap.h"
#include "config.h"
#include "dynamic_config_manager.h"
#include "private.h"
#include "tablet_balancer.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/server/lib/cypress_registrar/cypress_registrar.h>
#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/helpers.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/library/monitoring/http_integration.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressElection;
using namespace NNodeTrackerClient;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = TabletBalancerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(TTabletBalancerServerConfigPtr config, INodePtr configNode)
        : Config_(std::move(config))
        , ConfigNode_(std::move(configNode))
    {
        if (Config_->AbortOnUnrecognizedOptions) {
            AbortOnUnrecognizedOptions(Logger, Config_);
        } else {
            WarnForUnrecognizedOptions(Logger, Config_);
        }
    }

    void Run() override
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

    const NNative::IClientPtr& GetClient() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Client_;
    }

    const ICypressElectionManagerPtr& GetElectionManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ElectionManager_;
    }

    const IInvokerPtr& GetControlInvoker() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return ControlInvoker_;
    }

    const TDynamicConfigManagerPtr& GetDynamicConfigManager() const override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return DynamicConfigManager_;
    }

private:
    const TTabletBalancerServerConfigPtr Config_;
    const INodePtr ConfigNode_;

    ITabletBalancerPtr TabletBalancer_;

    TString LocalAddress_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    TActionQueuePtr ControlQueue_;
    IInvokerPtr ControlInvoker_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NCoreDump::ICoreDumperPtr CoreDumper_;

    NNative::IConnectionPtr Connection_;
    NNative::IClientPtr Client_;

    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    ICypressElectionManagerPtr ElectionManager_;

    TDynamicConfigManagerPtr DynamicConfigManager_;

    void DoRun();

    void RegisterInstance();
};

void TBootstrap::DoRun()
{
    YT_LOG_INFO("Starting tablet balancer process (ClusterName: %v)",
        Config_->ClusterConnection->Static->ClusterName);

    LocalAddress_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = NNative::CreateConnection(
        Config_->ClusterConnection,
        connectionOptions);

    NativeAuthenticator_ = NNative::CreateNativeAuthenticator(Connection_);

    auto clientOptions = TClientOptions::FromUser(Config_->ClusterUser);
    Client_ = Connection_->CreateNativeClient(clientOptions);

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    if (Config_->CoreDumper) {
        CoreDumper_ = CreateCoreDumper(Config_->CoreDumper);
    }

    DynamicConfigManager_ = New<TDynamicConfigManager>(Config_, this);
    DynamicConfigManager_->Start();

    TabletBalancer_ = CreateTabletBalancer(
        this,
        Config_->TabletBalancer,
        ControlInvoker_);

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

    if (CoreDumper_) {
        SetNodeByYPath(
            orchidRoot,
            "/core_dumper",
            CreateVirtualNode(CoreDumper_->CreateOrchidService()));
    }

    SetNodeByYPath(
        orchidRoot,
        "/tablet_balancer",
        CreateVirtualNode(TabletBalancer_->GetOrchidService()));

    SetNodeByYPath(
        orchidRoot,
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));

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

void TBootstrap::RegisterInstance()
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

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TTabletBalancerServerConfigPtr config, INodePtr configNode)
{
    return std::make_unique<TBootstrap>(std::move(config), std::move(configNode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
