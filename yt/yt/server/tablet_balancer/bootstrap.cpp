#include "bootstrap.h"
#include "config.h"
#include "tablet_balancer.h"

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/core_dump/core_dumper.h>

#include <yt/yt/server/lib/cypress_election/election_manager.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/misc/core_dumper.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NTabletBalancer {

using namespace NApi;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NCypressElection;
using namespace NYPath;
using namespace NYTree;

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

    const NApi::NNative::IClientPtr& GetMasterClient() override
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return Client_;
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
    ICoreDumperPtr CoreDumper_;

    NNative::IConnectionPtr Connection_;
    NNative::IClientPtr Client_;

    ICypressElectionManagerPtr ElectionManager_;

    void DoRun();

    void RegisterInstance();
};

void TBootstrap::DoRun()
{
    YT_LOG_INFO(
        "Starting tablet balancer process (NativeCluster: %v)",
        Config_->ClusterConnection->ClusterName);

    LocalAddress_ = NNet::BuildServiceAddress(NNet::GetLocalHostName(), Config_->RpcPort);

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = NNative::CreateConnection(
        Config_->ClusterConnection,
        connectionOptions);

    auto clientOptions = TClientOptions::FromUser(Config_->ClusterUser);
    Client_ = Connection_->CreateNativeClient(clientOptions);

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    if (Config_->CoreDumper) {
        CoreDumper_ = CreateCoreDumper(Config_->CoreDumper);
    }

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

    RpcServer_->RegisterService(NAdmin::CreateAdminService(
        ControlInvoker_,
        CoreDumper_));
    RpcServer_->RegisterService(NOrchid::CreateOrchidService(
        orchidRoot,
        ControlInvoker_));

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
}

void TBootstrap::RegisterInstance()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    auto instancePath = Format(
        "%v/instances/%v",
        Config_->RootPath,
        ToYPathLiteral(LocalAddress_));
    auto orchidPath = instancePath + "/orchid";

    NObjectClient::TObjectServiceProxy proxy(Client_
        ->GetMasterChannelOrThrow(EMasterChannelKind::Leader));
    auto batchReq = proxy.ExecuteBatch();

    {
        auto req = TCypressYPathProxy::Create(instancePath);
        req->set_ignore_existing(true);
        req->set_recursive(true);
        req->set_type(static_cast<int>(EObjectType::MapNode));
        GenerateMutationId(req);
        batchReq->AddRequest(req);
    }
    {
        auto req = TCypressYPathProxy::Create(orchidPath);
        req->set_ignore_existing(true);
        req->set_recursive(true);
        req->set_type(static_cast<int>(EObjectType::Orchid));
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("remote_addresses", LocalAddress_);
        ToProto(req->mutable_node_attributes(), *attributes);
        GenerateMutationId(req);
        batchReq->AddRequest(req);
    }

    YT_LOG_INFO(
        "Registering instance (Path: %Qv, OrchidPath: %Qv)",
        instancePath,
        orchidPath);

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError));

    YT_LOG_INFO("Orchid and instance nodes created");
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IBootstrap> CreateBootstrap(TTabletBalancerServerConfigPtr config, INodePtr configNode)
{
    return std::make_unique<TBootstrap>(std::move(config), std::move(configNode));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
