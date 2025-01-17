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

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/scheduler/config.h>

#include <yt/yt/ytlib/security_client/public.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/core/concurrency/fair_share_action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/net/address.h>
#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/yt/core/misc/proc.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

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
using namespace NFusion;
using namespace NLogging;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "SchedulerBoot");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TSchedulerBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ServiceLocator_(std::move(serviceLocator))
    , ControlQueue_(CreateEnumIndexedFairShareActionQueue<EControlQueue>("Control"))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), Config_);
    } else {
        WarnForUnrecognizedOptions(Logger(), Config_);
    }
}

TBootstrap::~TBootstrap() = default;

TFuture<void> TBootstrap::Run()
{
    return BIND(&TBootstrap::DoRun, MakeStrong(this))
        .AsyncVia(GetControlInvoker(EControlQueue::Default))
        .Run();
}

void TBootstrap::DoRun()
{
    DoInitialize();
    DoStart();
}

void TBootstrap::DoInitialize()
{
    YT_LOG_INFO("Starting scheduler");

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = true;
    Connection_ = NApi::NNative::CreateConnection(Config_->ClusterConnection, std::move(connectionOptions));

    NativeAuthenticator_ = NApi::NNative::CreateNativeAuthenticator(Connection_);

    auto clientOptions = TClientOptions::FromUser(NSecurityClient::SchedulerUserName);
    Client_ = Connection_->CreateNativeClient(clientOptions);

    NLogging::GetDynamicTableLogWriterFactory()->SetClient(Client_);

    Connection_->GetClusterDirectorySynchronizer()->Start();
    Connection_->GetMasterCellDirectorySynchronizer()->Start();

    BusServer_ = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(BusServer_);

    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    Scheduler_ = New<TScheduler>(Config_->Scheduler, this);

    ControllerAgentTracker_ = New<TControllerAgentTracker>(Config_->Scheduler, this);

    Scheduler_->Initialize();
    ControllerAgentTracker_->Initialize();

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        ServiceLocator_->GetServiceOrThrow<TSolomonExporterPtr>(),
        &MonitoringManager_,
        &orchidRoot);

    if (Config_->ExposeConfigInOrchid) {
        SetNodeByYPath(
            orchidRoot,
            "/config",
            CreateVirtualNode(ConfigNode_));
    }
    SetNodeByYPath(
        orchidRoot,
        "/scheduler",
        CreateVirtualNode(Scheduler_->CreateOrchidService()->Via(GetControlInvoker(EControlQueue::StaticOrchid))));
    SetBuildAttributes(
        orchidRoot,
        "scheduler");

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(EControlQueue::Default),
        ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(EControlQueue::StaticOrchid),
        NativeAuthenticator_));
    RpcServer_->RegisterService(CreateOperationService(this, Scheduler_->GetOperationServiceResponseKeeper()));
    RpcServer_->RegisterService(CreateAllocationTrackerService(this));
    RpcServer_->RegisterService(CreateControllerAgentTrackerService(this, ControllerAgentTracker_->GetResponseKeeper()));
}

void TBootstrap::DoStart()
{
    YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
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
    return NServer::GetLocalAddresses(Config_->Addresses, Config_->RpcPort);
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

void TBootstrap::Reconfigure(const TSchedulerConfigPtr& config)
{
    TSingletonManager::Reconfigure(config);

    RpcServer_->OnDynamicConfigChanged(config->RpcServer);
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateSchedulerBootstrap(
    TSchedulerBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    return New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
