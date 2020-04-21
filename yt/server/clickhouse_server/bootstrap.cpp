#include "bootstrap.h"

#include "stack_size_checker.h"

#include "private.h"

#include "host.h"

#include "clickhouse_service.h"
#include "config.h"
#include "query_context.h"
#include "query_registry.h"
#include "users_manager.h"

#include <yt/server/clickhouse_server/protos/clickhouse_service.pb.h>

#include <yt/server/lib/admin/admin_service.h>
#include <yt/server/lib/core_dump/core_dumper.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/program/build_attributes.h>
#include <yt/ytlib/program/configure_singletons.h>

#include <yt/ytlib/api/connection.h>
#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/client_cache.h>

#include <yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/client/api/client.h>
#include <yt/client/api/client_cache.h>

#include <yt/client/misc/discovery.h>

#include <yt/client/object_client/public.h>

#include <yt/client/node_tracker_client/public.h>

#include <yt/core/bus/tcp/server.h>
#include <yt/core/misc/crash_handler.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>
#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/logging/log_manager.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/core/misc/signal_registry.h>
#include <yt/core/misc/crash_handler.h>

#include <yt/core/ytalloc/statistics_producer.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/http/server.h>

#include <yt/core/rpc/bus/server.h>

#include <yt/core/net/local_address.h>

#include <yt/core/ytree/ephemeral_node_factory.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

#include <util/datetime/base.h>
#include <util/system/env.h>

namespace NYT::NClickHouseServer {

using namespace NAdmin;
using namespace NApi;
using namespace NApi::NNative;
using namespace NBus;
using namespace NConcurrency;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

/////////////////////////////////////////////////////////////////////////////

void WriteCurrentQueryIdToStderr()
{
    WriteToStderr("*** Current query id (possible reason of failure): ");
    const auto& queryId = DB::CurrentThread::getQueryId();
    WriteToStderr(queryId.data, queryId.size);
    WriteToStderr(" ***\n");
}

auto CreateNodeRequest(const TString& path, NObjectClient::EObjectType nodeType)
{
    auto request = NCypressClient::TCypressYPathProxy::Create(path);
    request->set_force(true);
    request->set_recursive(true);
    request->set_type(static_cast<int>(nodeType));
    GenerateMutationId(request);
    return request;
}

void InitOrchidNode(
    const TString& cypressRootPath,
    const TString& cliqueId,
    ui16 port,
    const NApi::NNative::IClientPtr& masterClient)
{
    const auto& localHostName = NNet::GetLocalHostName();
    const auto& jobCookie = GetEnv("YT_JOB_COOKIE");

    NObjectClient::TObjectServiceProxy proxy(masterClient->GetMasterChannelOrThrow(EMasterChannelKind::Leader));

    NNodeTrackerClient::TAddressMap rpcOrchidAddressMap;
    rpcOrchidAddressMap["default"] = NNet::GetLocalHostName() + ":" + std::to_string(port);

    auto orchidNodePath = cypressRootPath + "/orchids/" + cliqueId + "/" + jobCookie;

    auto orchidNodeCreateRequest = CreateNodeRequest(orchidNodePath, NObjectClient::EObjectType::Orchid);

    auto attributes = CreateEphemeralAttributes();
    attributes->Set("remote_addresses", rpcOrchidAddressMap);
    ToProto(orchidNodeCreateRequest->mutable_node_attributes(), *attributes);

    auto batchRequest = proxy.ExecuteBatch();
    batchRequest = proxy.ExecuteBatch();
    batchRequest->AddRequest(orchidNodeCreateRequest);

    auto batchResponseOrError = WaitFor(batchRequest->Invoke());

    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchResponseOrError), "Error initializing orchid node %v", orchidNodePath);

    YT_LOG_INFO("Initialized orchid node (LocalHostName: %v, Port: %v, CliqueId: %v, JobCookie: %v, OrchidNodePath: %v)",
        localHostName,
        port,
        cliqueId,
        jobCookie,
        orchidNodePath);
}

/////////////////////////////////////////////////////////////////////////////

// See the comment in stack_size_checker.h.
auto UnusedValue = IgnoreMe();

/////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TClickHouseServerBootstrapConfigPtr config,
    INodePtr configNode,
    TString instanceId,
    TString cliqueId,
    ui16 rpcPort,
    ui16 monitoringPort,
    ui16 tcpPort,
    ui16 httpPort)
    : Config_(std::move(config))
    , CliqueId_(std::move(cliqueId))
    , ConfigNode_(std::move(configNode))
    , InstanceId_(std::move(instanceId))
    , RpcPort_(rpcPort)
    , MonitoringPort_(monitoringPort)
    , TcpPort_(tcpPort)
    , HttpPort_(httpPort)
{
    WarnForUnrecognizedOptions(Logger, Config_);
}

void TBootstrap::Run()
{
    ControlQueue_ = New<TActionQueue>("Control");

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    YT_LOG_INFO("Starting ClickHouse server");

    // Make RSS predictable.
    NYTAlloc::SetEnableEagerMemoryRelease(true);

    Config_->MonitoringServer->Port = MonitoringPort_;
    Config_->MonitoringServer->ServerName = "monitoring";
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(HttpServer_, &MonitoringManager_, &orchidRoot);

    QueryRegistry_ = New<TQueryRegistry>(this);

    // Set up crash handlers.
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, [=] { QueryRegistry_->WriteStateToStderr(); });
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, NDetail::WriteCurrentQueryIdToStderr);
    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/queries",
        CreateVirtualNode(QueryRegistry_->GetOrchidService()->Via(GetControlInvoker())));
    SetBuildAttributes(orchidRoot, "clickhouse_server");

    // TODO(max42): make configurable.
    WorkerThreadPool_ = New<TThreadPool>(Config_->WorkerThreadCount, "Worker");
    WorkerInvoker_ = WorkerThreadPool_->GetInvoker();
    SerializedWorkerInvoker_ = CreateSerializedInvoker(WorkerInvoker_);

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    Config_->BusServer->Port = RpcPort_;
    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        CoreDumper_));

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    RpcServer_->RegisterService(CreateClickHouseService(this, InstanceId_));

    RpcServer_->Configure(Config_->RpcServer);

    NApi::NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;

    Connection_ = NApi::NNative::CreateConnection(
        Config_->ClusterConnection,
        connectionOptions);
    // Kick-start node directory synchronizing; otherwise it will start only with first query.
    Connection_->GetNodeDirectorySynchronizer()->Start();

    ClientCache_ = New<NApi::NNative::TClientCache>(Config_->ClientCache, Connection_);

    RootClient_ = ClientCache_->GetClient(Config_->User);
    CacheClient_ = ClientCache_->GetClient(CacheUserName);

    // Configure clique's directory.
    Config_->Discovery->Directory += "/" + CliqueId_;

    TCreateNodeOptions createCliqueNodeOptions;
    createCliqueNodeOptions.IgnoreExisting = true;
    createCliqueNodeOptions.Recursive = true;
    createCliqueNodeOptions.Attributes = ConvertToAttributes(
        THashMap<TString, i64>{{"discovery_version", TDiscovery::Version}});
    WaitFor(RootClient_->CreateNode(
        Config_->Discovery->Directory,
        NObjectClient::EObjectType::MapNode,
        createCliqueNodeOptions))
        .ThrowOnError();

    Host_ = New<TClickHouseHost>(
        this,
        Config_,
        CliqueId_,
        InstanceId_,
        RpcPort_,
        MonitoringPort_,
        TcpPort_,
        HttpPort_);
    QueryRegistry_->Start();

    if (HttpServer_) {
        YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
        HttpServer_->Start();
    }

    if (RpcServer_) {
        YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
        RpcServer_->Start();
    }


    NDetail::InitOrchidNode(Config_->Engine->CypressRootPath, CliqueId_, RpcPort_, RootClient_);

    Host_->Start();

    // Bootstrap never dies, so it is _kinda_ safe.
    TSignalRegistry::Get()->PushCallback(SIGINT, [=] { SigintHandler(); });
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

EInstanceState TBootstrap::GetState() const
{
    return SigintCounter_ == 0 ? EInstanceState::Active : EInstanceState::Stopped;
}

void TBootstrap::SigintHandler()
{
    ++SigintCounter_;
    if (SigintCounter_ > 1) {
        _exit(InterruptionExitCode);
    }
    YT_LOG_INFO("Stopping server due to SIGINT");
    Host_->StopDiscovery().Apply(BIND([this] {
        TDelayedExecutor::WaitForDuration(Config_->InterruptionGracefulTimeout);
        WaitFor(GetQueryRegistry()->GetIdleFuture()).ThrowOnError();
        QueryRegistry_->Stop();
        Host_->StopTcpServers();
        Y_UNUSED(WaitFor(RpcServer_->Stop()));
        MonitoringManager_->Stop();
        HttpServer_->Stop();
        TLogManager::StaticShutdown();
        _exit(InterruptionExitCode);
    }).Via(GetControlInvoker()));;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
