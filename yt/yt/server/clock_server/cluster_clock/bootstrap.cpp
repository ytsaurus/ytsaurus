#include "automaton.h"
#include "bootstrap.h"
#include "private.h"
#include "config.h"
#include "hydra_facade.h"

#include <yt/yt/server/lib/hydra/changelog.h>
#include <yt/yt/server/lib/hydra/local_changelog_store.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra/snapshot.h>

#include <yt/yt/server/lib/timestamp_server/timestamp_manager.h>

#include <yt/yt/server/lib/election/election_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/config.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/ytlib/election/cell_manager.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/client/transaction_client/timestamp_provider.h>

#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/net/local_address.h>

#include <yt/yt/core/http/server.h>

#include <yt/yt/library/fusion/service_locator.h>

#include <yt/yt/core/misc/ref_counted_tracker.h>
#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/ytree/ephemeral_node_factory.h>
#include <yt/yt/core/ytree/tree_builder.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NClusterClock {

using namespace NApi;
using namespace NAdmin;
using namespace NBus;
using namespace NRpc;
using namespace NNet;
using namespace NYTree;
using namespace NElection;
using namespace NHydra;
using namespace NHiveClient;
using namespace NTransactionClient;
using namespace NTimestampServer;
using namespace NMonitoring;
using namespace NOrchid;
using namespace NProfiling;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NYson;
using namespace NFusion;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Bootstrap");

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(
    TClusterClockBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
    , ServiceLocator_(std::move(serviceLocator))
{
    WarnForUnrecognizedOptions(Logger(), Config_);
}

TBootstrap::~TBootstrap() = default;

const TClusterClockBootstrapConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

TCellId TBootstrap::GetCellId() const
{
    return CellId_;
}

TCellTag TBootstrap::GetCellTag() const
{
    return CellTag_;
}

const IServerPtr& TBootstrap::GetRpcServer() const
{
    return RpcServer_;
}

const IChannelPtr& TBootstrap::GetLocalRpcChannel() const
{
    return LocalRpcChannel_;
}

const TCellManagerPtr& TBootstrap::GetCellManager() const
{
    return CellManager_;
}

const IChangelogStoreFactoryPtr& TBootstrap::GetChangelogStoreFactory() const
{
    return ChangelogStoreFactory_;
}

const ISnapshotStorePtr& TBootstrap::GetSnapshotStore() const
{
    return SnapshotStore_;
}

const THydraFacadePtr& TBootstrap::GetHydraFacade() const
{
    return HydraFacade_;
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return ControlQueue_->GetInvoker();
}

const IInvokerPtr& TBootstrap::GetSnapshotIOInvoker() const
{
    return SnapshotIOQueue_->GetInvoker();
}

void TBootstrap::Initialize()
{
    ControlQueue_ = New<TActionQueue>("Control");
    SnapshotIOQueue_ = New<TActionQueue>("SnapshotIO");

    BIND(&TBootstrap::DoInitialize, MakeStrong(this))
        .AsyncVia(GetControlInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TFuture<void> TBootstrap::Run()
{
    return BIND(&TBootstrap::DoRun, MakeStrong(this))
        .AsyncVia(GetControlInvoker())
        .Run();
}

void TBootstrap::LoadSnapshot(const TString& fileName, bool dump)
{
    BIND(&TBootstrap::DoLoadSnapshot, MakeStrong(this), fileName, dump)
        .AsyncVia(HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::Default))
        .Run()
        .Get()
        .ThrowOnError();
}

void TBootstrap::DoInitialize()
{
    Config_->ClockCell->ValidateAllPeersPresent();

    auto localAddress = BuildServiceAddress(GetLocalHostName(), Config_->RpcPort);

    // TODO(gepardo): Possibly add authentication here.

    TCellConfigPtr localCellConfig;
    int localPeerId;

    auto primaryId = Config_->ClockCell->GetPeerIdOrThrow(localAddress);
    localCellConfig = Config_->ClockCell;
    localPeerId = primaryId;

    CellId_ = localCellConfig->CellId;
    CellTag_ = CellTagFromId(CellId_);

    YT_LOG_INFO("Running clock server (CellId: %v, CellTag: %v, PeerId: %v)",
        CellId_,
        CellTag_,
        localPeerId);

    auto channelFactory = CreateCachingChannelFactory(NRpc::NBus::CreateTcpBusChannelFactory(Config_->BusClient));

    auto busServer = CreateBusServer(Config_->BusServer);

    RpcServer_ = NRpc::NBus::CreateBusServer(busServer);

    LocalRpcChannel_ = CreateRealmChannel(
        CreateLocalChannel(RpcServer_),
        CellId_);

    CellManager_ = New<TCellManager>(
        localCellConfig,
        channelFactory,
        nullptr,
        localPeerId);

    ChangelogStoreFactory_ = CreateLocalChangelogStoreFactory(
        Config_->Changelogs,
        "ChangelogFlush",
        NProfiling::TProfiler("/changelogs"));

    auto snapshotStoreFuture = CreateLocalSnapshotStore(
        Config_->Snapshots,
        GetSnapshotIOInvoker());
    auto snapshotStore = WaitFor(snapshotStoreFuture)
        .ValueOrThrow();
    SnapshotStore_ = snapshotStore;

    HydraFacade_ = New<THydraFacade>(Config_, this);

    auto timestampManager = New<TTimestampManager>(
        Config_->TimestampManager,
        HydraFacade_->GetAutomatonInvoker(EAutomatonThreadQueue::TimestampManager),
        HydraFacade_->GetHydraManager(),
        HydraFacade_->GetAutomaton(),
        GetCellTag(),
        /*authenticator*/ nullptr,
        Config_->ClockClusterTag);

    RpcServer_->RegisterService(timestampManager->GetRpcService()); // null realm
    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        ServiceLocator_->FindService<NCoreDump::ICoreDumperPtr>(),
        /*authenticator*/ nullptr));

    RpcServer_->Configure(Config_->RpcServer);
}

void TBootstrap::DoRun()
{
    HydraFacade_->Initialize();

    YT_LOG_INFO("Listening for HTTP requests (Port: %v)", Config_->MonitoringPort);
    HttpServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        HttpServer_,
        ServiceLocator_->GetServiceOrThrow<NProfiling::TSolomonExporterPtr>(),
        &MonitoringManager_,
        &orchidRoot);

    MonitoringManager_->Register(
        "/hydra",
        HydraFacade_->GetHydraManager()->GetMonitoringProducer());
    MonitoringManager_->Register(
        "/election",
        HydraFacade_->GetElectionManager()->GetMonitoringProducer());

    if (Config_->ExposeConfigInOrchid) {
        SetNodeByYPath(
            orchidRoot,
            "/config",
            CreateVirtualNode(ConfigNode_));
    }

    SetBuildAttributes(
        orchidRoot,
        "clock");

    SetNodeByYPath(
        orchidRoot,
        "/clock_cell",
        CreateVirtualNode(CreateCellOrchidService()));

    HttpServer_->Start();

    YT_LOG_INFO("Listening for RPC requests (Port: %v)", Config_->RpcPort);
    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(),
        /*authenticator*/ nullptr));
    RpcServer_->Start();
}

void TBootstrap::DoLoadSnapshot(const TString& fileName, bool dump)
{
    auto reader = CreateLocalSnapshotReader(
        fileName,
        InvalidSegmentId,
        GetSnapshotIOInvoker());
    HydraFacade_->LoadSnapshot(reader, dump);
}

IYPathServicePtr TBootstrap::CreateCellOrchidService() const
{
    return New<TCompositeMapService>()
        ->AddAttribute(EInternedAttributeKey::Opaque, BIND([] (IYsonConsumer* consumer) {
            BuildYsonFluently(consumer)
                .Value(true);
            }))
        ->AddChild("hydra", IYPathService::FromProducer(
            HydraFacade_->GetHydraManager()->GetMonitoringProducer()))
        ->AddChild("election", IYPathService::FromProducer(
            HydraFacade_->GetElectionManager()->GetMonitoringProducer()));
}

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateClusterClockBootstrap(
    TClusterClockBootstrapConfigPtr config,
    INodePtr configNode,
    IServiceLocatorPtr serviceLocator)
{
    auto bootstrap = New<TBootstrap>(
        std::move(config),
        std::move(configNode),
        std::move(serviceLocator));
    bootstrap->Initialize();
    return bootstrap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
