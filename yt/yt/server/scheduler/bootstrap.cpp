#include "bootstrap.h"

#include "allocation_tracker_service.h"
#include "private.h"
#include "private.h"
#include "scheduler.h"
#include "scheduler_service.h"
#include "controller_agent_tracker_service.h"
#include "controller_agent_tracker.h"

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/misc/address_helpers.h>

#include <yt/yt/server/lib/scheduler/config.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/library/coredumper/coredumper.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/response_keeper.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NScheduler {

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
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::Run()
{
    ControlQueue_ = CreateEnumIndexedFairShareActionQueue<EControlQueue>("Control");

    BIND(&TBootstrap::DoRun, this)
        .AsyncVia(GetControlInvoker(EControlQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();

    Sleep(TDuration::Max());
}

void TBootstrap::DoRun()
{
    YT_LOG_INFO("Starting scheduler");

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection, std::move(connectionOptions));

    NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

    auto clientOptions = TClientOptions::FromUser(NSecurityClient::SchedulerUserName);
    Client_ = Connection_->CreateNativeClient(clientOptions);

    Connection_->GetClusterDirectorySynchronizer()->Start();

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    Scheduler_ = New<TScheduler>(Config_->Scheduler, this);

    ControllerAgentTracker_ = New<TControllerAgentTracker>(Config_->Scheduler, this);

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    Scheduler_->Initialize();
    ControllerAgentTracker_->Initialize();

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
        "/scheduler",
        CreateVirtualNode(Scheduler_->CreateOrchidService()->Via(GetControlInvoker(EControlQueue::StaticOrchid))));
    SetBuildAttributes(
        orchidRoot,
        "scheduler");

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(EControlQueue::Default),
        CoreDumper_,
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(EControlQueue::StaticOrchid),
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOperationService(this, Scheduler_->GetOperationServiceResponseKeeper()));
    RpcServer_->RegisterService(CreateAllocationTrackerService(this));
    RpcServer_->RegisterService(CreateControllerAgentTrackerService(this, ControllerAgentTracker_->GetResponseKeeper()));

    YT_LOG_INFO("Listening for HTTP requests on port %v", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests on port %v", Config_->RpcPort);
    RpcServer_->Configure(Config_->RpcServer);
    RpcServer_->Start();
}

const TSchedulerBootstrapConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

const NNative::IClientPtr& TBootstrap::GetClient() const
{
    return Client_;
}

const NNative::IClientPtr& TBootstrap::GetRemoteClient(TCellTag tag) const
{
    auto it = RemoteClients_.find(tag);
    if (it == RemoteClients_.end()) {
        auto connection = NNative::GetRemoteConnectionOrThrow(Client_->GetNativeConnection(), tag);
        auto client = connection->CreateNativeClient(TClientOptions::FromUser(NSecurityClient::SchedulerUserName));
        auto result = RemoteClients_.emplace(tag, client);
        YT_VERIFY(result.second);
        it = result.first;
    }
    return it->second;
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
    return ControlQueue_->GetInvoker(queue);
}

const TSchedulerPtr& TBootstrap::GetScheduler() const
{
    return Scheduler_;
}

const TControllerAgentTrackerPtr& TBootstrap::GetControllerAgentTracker() const
{
    return ControllerAgentTracker_;
}

const NRpc::IAuthenticatorPtr& TBootstrap::GetNativeAuthenticator() const
{
    return NativeAuthenticator_;
}

void TBootstrap::OnDynamicConfigChanged(const TSchedulerConfigPtr& config)
{
    ReconfigureNativeSingletons(Config_, config);

    RpcServer_->OnDynamicConfigChanged(config->RpcServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
