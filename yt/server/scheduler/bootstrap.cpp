#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/server/admin_server/admin_service.h>

#include <yt/server/misc/address_helpers.h>

#include <yt/server/scheduler/config.h>
#include <yt/server/scheduler/job_prober_service.h>
#include <yt/server/scheduler/job_tracker_service.h>
#include <yt/server/scheduler/private.h>
#include <yt/server/scheduler/scheduler.h>
#include <yt/server/scheduler/scheduler_service.h>
#include <yt/server/scheduler/controller_agent_tracker_service.h>
#include <yt/server/scheduler/controller_agent_tracker.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/api/native_client.h>
#include <yt/ytlib/api/native_connection.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/security_client/public.h>

#include <yt/ytlib/core_dump/core_dumper.h>

#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/http/server.h>

#include <yt/core/concurrency/fair_share_action_queue.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/net/address.h>
#include <yt/core/net/local_address.h>

#include <yt/core/misc/core_dumper.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/core/misc/lfalloc_helpers.h>
#include <yt/core/misc/proc.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>
#include <yt/core/rpc/response_keeper.h>
#include <yt/core/rpc/retrying_channel.h>
#include <yt/core/rpc/server.h>

#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

namespace NYT {
namespace NScheduler {

using namespace NAdmin;
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
using namespace NYTree;
using namespace NConcurrency;
using namespace NHiveClient;
using namespace NApi;
using namespace NNodeTrackerClient;
using namespace NControllerAgent;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SchedulerLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TSchedulerBootstrapConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    WarnForUnrecognizedOptions(Logger, Config_);
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    ControlQueue_ = New<TFairShareActionQueue>("Control", TEnumTraits<EControlQueue>::GetDomainNames());

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker(EControlQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    LOG_INFO("Starting scheduler");

    TNativeConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = CreateNativeConnection(Config_->ClusterConnection, connectionOptions);

    TClientOptions clientOptions;
    clientOptions.User = NSecurityClient::SchedulerUserName;
    Client_ = Connection_->CreateNativeClient(clientOptions);

    BusServer_ = CreateTcpBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->BindRetryCount = Config_->BusServer->BindRetryCount;
    Config_->MonitoringServer->BindRetryBackoff = Config_->BusServer->BindRetryBackoff;
    HttpServer_ = NHttp::CreateServer(Config_->MonitoringServer);

    Scheduler_ = New<TScheduler>(Config_->Scheduler, this);

    ControllerAgentTracker_ = New<TControllerAgentTracker>(Config_->Scheduler, this);

    ResponseKeeper_ = New<TResponseKeeper>(
        Config_->ResponseKeeper,
        GetControlInvoker(EControlQueue::UserRequest),
        SchedulerLogger,
        SchedulerProfiler);

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());
    MonitoringManager_->Start();

    LFAllocProfiler_ = std::make_unique<NLFAlloc::TLFAllocProfiler>();

    Scheduler_->Initialize();
    ControllerAgentTracker_->Initialize();

    auto orchidRoot = NYTree::GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetNodeByYPath(
        orchidRoot,
        "/scheduler",
        CreateVirtualNode(Scheduler_->GetOrchidService()));

    SetBuildAttributes(orchidRoot, "scheduler");

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(EControlQueue::Default),
        CoreDumper_));

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(EControlQueue::Orchid)));

    HttpServer_->AddHandler(
        "/orchid/",
        NMonitoring::GetOrchidYPathHttpHandler(orchidRoot->Via(GetControlInvoker(EControlQueue::Orchid))));

    RpcServer_->RegisterService(CreateSchedulerService(this));
    RpcServer_->RegisterService(CreateJobTrackerService(this));
    RpcServer_->RegisterService(CreateJobProberService(this));
    RpcServer_->RegisterService(CreateControllerAgentTrackerService(this));

    LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();
}

const TSchedulerBootstrapConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const INativeClientPtr& TBootstrap::GetMasterClient() const
{
    return Client_;
}

TAddressMap TBootstrap::GetLocalAddresses() const
{
    return NYT::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);
}

TNetworkPreferenceList TBootstrap::GetLocalNetworks() const
{
    return Config_->Addresses.empty()
        ? DefaultNetworkPreferences
        : GetIths<0>(Config_->Addresses);
}

IInvokerPtr TBootstrap::GetControlInvoker(EControlQueue queue) const
{
    return ControlQueue_->GetInvoker(static_cast<int>(queue));
}

const TSchedulerPtr& TBootstrap::GetScheduler() const
{
    return Scheduler_;
}

const TControllerAgentTrackerPtr& TBootstrap::GetControllerAgentTracker() const
{
    return ControllerAgentTracker_;
}

const TResponseKeeperPtr& TBootstrap::GetResponseKeeper() const
{
    return ResponseKeeper_;
}

const ICoreDumperPtr& TBootstrap::GetCoreDumper() const
{
    return CoreDumper_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
