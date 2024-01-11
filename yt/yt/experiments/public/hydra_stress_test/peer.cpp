#include "peer.h"
#include "peer_proxy.h"

#include <yt/yt/server/lib/hydra/local_changelog_store.h>
#include <yt/yt/server/lib/hydra/local_snapshot_store.h>
#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>

#include <yt/yt/server/lib/election/distributed_election_manager.h>
#include <yt/yt/server/lib/election/election_manager_thunk.h>

#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/local_channel.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/logging/config.h>

#include <yt/yt/ytlib/election/cell_manager.h>

namespace NYT::NHydraStressTest {

using namespace NConcurrency;
using namespace NElection;
using namespace NHydra;
using namespace NRpc;
using namespace NYTree;

//////////////////////////////////////////////////////////////////////////////////

class TPeerService
    : public THydraServiceBase
{
public:
    explicit TPeerService(TPeerPtr peer)
        : THydraServiceBase(
            peer->HydraManager_,
            peer->GetAutomatonInvoker(),
            TPeerServiceProxy::GetDescriptor(),
            HydraStressTestLogger,
            NullRealmId,
            CreateHydraManagerUpstreamSynchronizer(peer->HydraManager_),
            /*authenticator*/ nullptr)
        , Peer_(peer)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Read));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Cas));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Sequence));
    }

private:
    const TPeerPtr Peer_;

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, Read)
    {
        Y_UNUSED(request);
        context->SetRequestInfo();
        ValidatePeer(EPeerKind::LeaderOrFollower);
        SyncWithUpstream();

        auto value = Peer_->AutomatonPart_->GetCasValue();
        response->set_result(value);
        context->SetResponseInfo("Result: %v", value);
        TDelayedExecutor::WaitForDuration(RandomDuration(TDuration::Seconds(3)));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, Cas)
    {
        Y_UNUSED(response);
        ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo("Expected: %v, Desired: %v",
            request->expected(),
            request->desired());
        auto future = Peer_->AutomatonPart_->CreateCasMutation(context)->CommitAndReply(context);
    }

    DECLARE_RPC_SERVICE_METHOD(NYT::NProto, Sequence)
    {
        Y_UNUSED(response);
        ValidatePeer(EPeerKind::Leader);

        context->SetRequestInfo("Count: %v, SequenceId: %v",
            request->count(),
            request->id());
        auto future = Peer_->AutomatonPart_->CreateSequenceMutation(context)->CommitAndReply(context);
    }
};

DEFINE_REFCOUNTED_TYPE(TPeerService)

//////////////////////////////////////////////////////////////////////////////////

TPeer::TPeer(
    TConfigPtr config,
    TCellConfigPtr cellConfig,
    IChannelFactoryPtr channelFactory,
    int peerId,
    IInvokerPtr snapshotIOInvoker,
    TLinearizabilityCheckerPtr linearizabilityChecker,
    bool voting)
    : Config_(config)
    , CellConfig_(cellConfig)
    , SnapshotIOInvoker_(snapshotIOInvoker)
    , LinearizabilityChecker_(linearizabilityChecker)
    , PeerId_(peerId)
    , Voting_(voting)
    , CellManager_(New<TCellManager>(
        CellConfig_,
        channelFactory,
        nullptr,
        PeerId_))
    , AutomatonQueue_(New<TActionQueue>(Format("Automaton%v", PeerId_)))
    , ControlQueue_(New<TActionQueue>(Format("Control%v", PeerId_)))
    , ElectionManagerThunk_(New<TElectionManagerThunk>())
    , RpcServer_(CreateLocalServer())
    , Channel_(CreateLocalChannel(RpcServer_))
{
    RpcServer_->Start();
    // NB: not creating peer service here as that would require access to Hydra manager.
}

void TPeer::Initialize()
{
    auto automaton = New<TAutomaton>();

    auto changelogsConfig = CloneYsonStruct(Config_->Changelogs);
    changelogsConfig->Path += "/" + ToString(PeerId_);
    auto changelogStoreFactory = CreateLocalChangelogStoreFactory(
        changelogsConfig,
        Format("ChangelogFlush:%v", PeerId_),
        {});

    auto snapshotsConfig = CloneYsonStruct(Config_->Snapshots);
    snapshotsConfig->Path += "/" + ToString(PeerId_);

    auto snapshotStore = WaitFor(CreateLocalSnapshotStore(snapshotsConfig, SnapshotIOInvoker_))
        .ValueOrThrow();

    HydraManager_ = NHydra::CreateDistributedHydraManager(
        Config_->HydraManager,
        ControlQueue_->GetInvoker(),
        AutomatonQueue_->GetInvoker(),
        automaton,
        RpcServer_,
        ElectionManagerThunk_,
        CellManager_->GetCellId(),
        changelogStoreFactory,
        snapshotStore,
        /*authenticator*/ nullptr,
        {});

    AutomatonPart_ = New<TAutomatonPart>(
        HydraManager_,
        automaton,
        AutomatonQueue_->GetInvoker(),
        LinearizabilityChecker_);

    auto electionManager = CreateDistributedElectionManager(
        Config_->ElectionManager,
        CellManager_,
        ControlQueue_->GetInvoker(),
        HydraManager_->GetElectionCallbacks(),
        RpcServer_,
        /*authenticator*/ nullptr);
    ControlQueue_->GetInvoker()->Invoke(BIND(&IElectionManager::Initialize, electionManager));
    ElectionManagerThunk_->SetUnderlying(electionManager);

    PeerService_ = New<TPeerService>(this);
    RpcServer_->RegisterService(PeerService_);

    ControlQueue_->GetInvoker()->Invoke(BIND(&IHydraManager::Initialize, HydraManager_));
}

TFuture<void> TPeer::Finalize()
{
    if (PeerService_) {
        RpcServer_->UnregisterService(PeerService_);
        PeerService_.Reset();
    }

    auto result = BIND(&IHydraManager::Finalize, HydraManager_)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run();

    YT_UNUSED_FUTURE(BIND(&TElectionManagerThunk::Finalize, ElectionManagerThunk_)
        .AsyncVia(ControlQueue_->GetInvoker())
        .Run());

    return result;
}

bool TPeer::IsActive() const
{
    return HydraManager_->IsActive();
}

bool TPeer::IsRecovery() const
{
    return BIND(&IHydraManager::IsRecovery, HydraManager_)
        .AsyncVia(AutomatonQueue_->GetInvoker())
        .Run()
        .Get()
        .ValueOrThrow();
}

bool TPeer::IsVoting() const
{
    return Voting_;
}

NHydra::EPeerState TPeer::GetAutomatonState() const
{
    return BIND(&IHydraManager::GetAutomatonState, HydraManager_)
        .AsyncVia(AutomatonQueue_->GetInvoker())
        .Run()
        .Get()
        .ValueOrThrow();
}

IChannelPtr TPeer::GetChannel() const
{
    return Channel_;
}

int TPeer::GetPeerId() const
{
    return PeerId_;
}

IInvokerPtr TPeer::GetAutomatonInvoker() const
{
    return AutomatonQueue_->GetInvoker();
}

//////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydraStressTest
