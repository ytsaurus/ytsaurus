#include "stdafx.h"
#include "election_manager.h"
#include "config.h"
#include "election_service_proxy.h"
#include "cell_manager.h"
#include "private.h"

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/thread_affinity.h>

#include <ytlib/actions/parallel_awaiter.h>

#include <ytlib/ytree/fluent.h>

#include <ytlib/rpc/service_detail.h>
#include <ytlib/rpc/server.h>

namespace NYT {
namespace NElection {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = ElectionLogger;
static auto& Profiler = ElectionProfiler;

////////////////////////////////////////////////////////////////////////////////

TEpochContext::TEpochContext()
    : LeaderId(InvalidPeerId)
    , CancelableContext(New<TCancelableContext>())
{ }

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TImpl
    : public NRpc::TServiceBase
{
public:
    TImpl(
        TElectionManagerConfigPtr config,
        TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker,
        IElectionCallbacksPtr electionCallbacks,
        NRpc::IServerPtr rpcServer);

    void Start();
    void Stop();
    void Restart();

    void GetMonitoringInfo(IYsonConsumer* consumer);

    TEpochContextPtr GetEpochContext();

private:
    typedef TImpl TThis;

    class TVotingRound;
    typedef TIntrusivePtr<TVotingRound> TVotingRoundPtr;

    class TFollowerPinger;
    typedef TIntrusivePtr<TFollowerPinger> TFollowerPingerPtr;


    EPeerState State;

    // Voting parameters.
    TPeerId VoteId;
    TEpochId VoteEpochId;

    // Epoch parameters.
    TEpochContextPtr EpochContext;
    IInvokerPtr ControlEpochInvoker;

    typedef yhash_set<TPeerId> TPeerSet;
    TPeerSet AliveFollowers;
    TPeerSet PotentialFollowers;

    TDelayedInvoker::TCookie PingTimeoutCookie;
    TFollowerPingerPtr FollowerPinger;

    TElectionManagerConfigPtr Config;
    TCellManagerPtr CellManager;
    IInvokerPtr ControlInvoker;
    IElectionCallbacksPtr ElectionCallbacks;

    // Corresponds to #ControlInvoker.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);

    void Reset();
    void OnLeaderPingTimeout();

    void DoStart();
    void DoStop();

    void StartVotingRound();
    void StartVoteFor(TPeerId voteId, const TEpochId& voteEpoch);
    void StartVoting();

    void StartLeading();
    void StartFollowing(TPeerId leaderId, const TEpochId& epoch);
    void StopLeading();
    void StopFollowing();

    void InitEpochContext(TPeerId leaderId, const TEpochId& epoch);
    void SetState(EPeerState newState);

};

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TImpl::TFollowerPinger
    : public TRefCounted
{
public:
    explicit TFollowerPinger(TImplPtr owner)
        : Owner(owner)
        , Awaiter(New<TParallelAwaiter>(Owner->ControlEpochInvoker))
    { }

    void Start()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        auto& cellManager = Owner->CellManager;
        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id != cellManager->GetSelfId()) {
                SendPing(id);
            }
        }
    }

    void Stop()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        // Do nothing.
    }

private:
    TImplPtr Owner;
    TParallelAwaiterPtr Awaiter;

    void SendPing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (Awaiter->IsCanceled())
            return;

        LOG_DEBUG("Sending ping to follower %d", id);

        TElectionServiceProxy proxy(Owner->CellManager->GetMasterChannel(id));
        auto request = proxy
            .PingFollower()
            ->SetTimeout(Owner->Config->RpcTimeout);
        request->set_leader_id(Owner->CellManager->GetSelfId());
        ToProto(request->mutable_epoch_id(), Owner->EpochContext->EpochId);

        Awaiter->Await(
            request->Invoke(),
            BIND(&TFollowerPinger::OnPingResponse, MakeStrong(this), id)
                .Via(Owner->ControlEpochInvoker));
    }

    void SchedulePing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        TDelayedInvoker::Submit(
            BIND(&TFollowerPinger::SendPing, MakeStrong(this), id)
                .Via(Owner->ControlEpochInvoker),
            Owner->Config->FollowerPingInterval);
    }

    void OnPingResponse(TPeerId id, TElectionServiceProxy::TRspPingFollowerPtr response)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);
        YCHECK(Owner->State == EPeerState::Leading);

        if (!response->IsOK()) {
            auto error = response->GetError();
            if (NRpc::IsRpcError(error)) {
                // Hard error
                if (Owner->AliveFollowers.erase(id) > 0) {
                    LOG_WARNING(error, "Error pinging follower %d, considered down",
                        id);
                    Owner->PotentialFollowers.erase(id);
                }
            } else {
                // Soft error
                if (Owner->PotentialFollowers.find(id) ==
                    Owner->PotentialFollowers.end())
                {
                    if (Owner->AliveFollowers.erase(id) > 0) {
                        LOG_WARNING(error, "Error pinging follower %d, considered down",
                            id);
                    }
                } else {
                    if (TInstant::Now() > Owner->EpochContext->StartTime + Owner->Config->PotentialFollowerTimeout) {
                        LOG_WARNING(error, "Error pinging follower %d, no success within timeout, considered down",
                            id);
                        Owner->PotentialFollowers.erase(id);
                        Owner->AliveFollowers.erase(id);
                    } else {
                        LOG_INFO(error, "Error pinging follower %d, will retry later",
                            id);
                    }
                }
            }

            if (static_cast<int>(Owner->AliveFollowers.size()) < Owner->CellManager->GetQuorum()) {
                LOG_WARNING("Quorum is lost");
                Owner->StopLeading();
                Owner->StartVoting();
                return;
            }

            if (response->GetError().GetCode() == NRpc::EErrorCode::Timeout) {
                SendPing(id);
            } else {
                SchedulePing(id);
            }

            return;
        }

        LOG_DEBUG("Ping reply received from follower %d", id);

        if (Owner->PotentialFollowers.find(id) !=
            Owner->PotentialFollowers.end())
        {
            LOG_INFO("Follower %d is up, first success", id);
            Owner->PotentialFollowers.erase(id);
        } else if (Owner->AliveFollowers.find(id) ==
                 Owner->AliveFollowers.end())
        {
            LOG_INFO("Follower %d is up", id);
            Owner->AliveFollowers.insert(id);
        }

        SchedulePing(id);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TImpl::TVotingRound
    : public TRefCounted
{
public:
    explicit TVotingRound(TImplPtr owner)
        : Owner(owner)
        , ControlEpochInvoker(owner->ControlEpochInvoker)
        , Awaiter(New<TParallelAwaiter>(ControlEpochInvoker))
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);
        YCHECK(Owner->State == EPeerState::Voting);

        auto callbacks = Owner->ElectionCallbacks;
        auto cellManager = Owner->CellManager;
        auto priority = callbacks->GetPriority();

        LOG_DEBUG("New voting round started (Round: %p, VoteId: %d, Priority: %s, VoteEpochId: %s)",
            this,
            Owner->VoteId,
            ~callbacks->FormatPriority(priority),
            ~ToString(Owner->VoteEpochId));

        ProcessVote(
            cellManager->GetSelfId(),
            TStatus(
                Owner->State,
                Owner->VoteId,
                priority,
                Owner->VoteEpochId));

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            TElectionServiceProxy proxy(cellManager->GetMasterChannel(id));
            proxy.SetDefaultTimeout(Owner->Config->RpcTimeout);
            auto request = proxy.GetStatus();
            Awaiter->Await(
                request->Invoke(),
                BIND(&TVotingRound::OnResponse, MakeStrong(this), id));
        }

        Awaiter->Complete(BIND(&TVotingRound::OnComplete, MakeStrong(this)));
    }

private:
    struct TStatus
    {
        EPeerState State;
        TPeerId VoteId;
        TPeerPriority Priority;
        TEpochId VoteEpochId;

        TStatus(
            EPeerState state = EPeerState::Stopped,
            TPeerId vote = InvalidPeerId,
            TPeerPriority priority = -1,
            const TEpochId& voteEpochId = TEpochId())
            : State(state)
            , VoteId(vote)
            , Priority(priority)
            , VoteEpochId(voteEpochId)
        { }
    };

    typedef yhash_map<TPeerId, TStatus> TStatusTable;

    TImplPtr Owner;
    IInvokerPtr ControlEpochInvoker;
    TParallelAwaiterPtr Awaiter;
    TStatusTable StatusTable;

    bool ProcessVote(TPeerId id, const TStatus& status)
    {
        StatusTable[id] = status;
        return CheckForLeader();
    }

    void OnResponse(TPeerId id, TElectionServiceProxy::TRspGetStatusPtr response)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (!response->IsOK()) {
            LOG_INFO(response->GetError(), "Error requesting status from peer %d (Round: %p)",
                id,
                this);
            return;
        }

        auto state = EPeerState(response->state());
        auto vote = response->vote_id();
        auto priority = response->priority();
        auto epochId = FromProto<TEpochId>(response->vote_epoch_id());

        LOG_DEBUG("Received status from peer %d (Round: %p, State: %s, VoteId: %d, Priority: %s, VoteEpochId: %s)",
            id,
            this,
            ~state.ToString(),
            vote,
            ~Owner->ElectionCallbacks->FormatPriority(priority),
            ~ToString(epochId));

        ProcessVote(id, TStatus(state, vote, priority, epochId));
    }

    bool CheckForLeader()
    {
        LOG_DEBUG("Checking candidates (Round: %p)", this);

        FOREACH (const auto& pair, StatusTable) {
            if (CheckForLeader(pair.first, pair.second)) {
                return true;
            }
        }

        LOG_DEBUG("No leader candidate found (Round: %p)", this);

        return false;
    }

    bool CheckForLeader(
        TPeerId candidateId,
        const TStatus& candidateStatus)
    {
        if (!IsFeasibleCandidate(candidateId, candidateStatus)) {
            LOG_DEBUG("Candidate %d is not feasible (Round: %p)",
                candidateId,
                this);
            return false;
        }

        // Compute candidate epoch.
        // Use the local one for self
        // (others may still be following with an outdated epoch).
        auto candidateEpochId =
            candidateId == Owner->CellManager->GetSelfId()
            ? Owner->VoteEpochId
            : candidateStatus.VoteEpochId;

        // Count votes (including self) and quorum.
        int voteCount = CountVotesFor(candidateId, candidateEpochId);
        int quorum = Owner->CellManager->GetQuorum();

        // Check for quorum.
        if (voteCount < quorum) {
            LOG_DEBUG("Candidate %d has too few votes: %d < %d (Round: %p, VoteEpochId: %s)",
                candidateId,
                voteCount,
                quorum,
                this,
                ~ToString(candidateEpochId));
            return false;
        }

        LOG_DEBUG("Candidate %d has quorum: %d >= %d (Round: %p, VoteEpochId: %s)",
            candidateId,
            voteCount,
            quorum,
            this,
            ~ToString(candidateEpochId));

        Awaiter->Cancel();

        // Become a leader or a follower.
        if (candidateId == Owner->CellManager->GetSelfId()) {
            ControlEpochInvoker->Invoke(BIND(
                &TThis::StartLeading,
                Owner));
        } else {
            ControlEpochInvoker->Invoke(BIND(
                &TThis::StartFollowing,
                Owner,
                candidateId,
                candidateStatus.VoteEpochId));
        }

        return true;
    }

    int CountVotesFor(TPeerId candidateId, const TEpochId& epochId) const
    {
        int result = 0;
        FOREACH (const auto& pair, StatusTable) {
            if (pair.second.VoteId == candidateId && pair.second.VoteEpochId == epochId) {
                ++result;
            }
        }
        return result;
    }

    bool IsFeasibleCandidate(
        TPeerId candidateId,
        const TStatus& candidateStatus) const
    {
        // He must be voting for himself.
        if (candidateId != candidateStatus.VoteId)
            return false;

        if (candidateId == Owner->CellManager->GetSelfId()) {
            // Check that we're voting.
            YCHECK(candidateStatus.State == EPeerState::Voting);
            return true;
        } else {
            // The candidate must be aware of his leadership.
            return candidateStatus.State == EPeerState::Leading;
        }
    }

    // Compare votes lexicographically by (priority, id).
    bool IsBetterCandidate(const TStatus& lhs, const TStatus& rhs) const
    {
        if (lhs.Priority > rhs.Priority)
            return true;

        if (lhs.Priority < rhs.Priority)
            return false;

        return lhs.VoteId < rhs.VoteId;
    }

    void ChooseVote()
    {
        // Choose the best vote.
        TStatus bestCandidate;
        FOREACH (const auto& pair, StatusTable) {
            const TStatus& currentCandidate = pair.second;
            if (StatusTable.find(currentCandidate.VoteId) != StatusTable.end() &&
                IsBetterCandidate(currentCandidate, bestCandidate))
            {
                bestCandidate = currentCandidate;
            }
        }

        // Extract the status of the best candidate.
        // His status must be present in the table by the above checks.
        const TStatus& candidateStatus = StatusTable[bestCandidate.VoteId];
        Owner->StartVoteFor(candidateStatus.VoteId, candidateStatus.VoteEpochId);
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        LOG_DEBUG("Voting round completed (Round: %p)", this);

        ChooseVote();
    }

};

////////////////////////////////////////////////////////////////////////////////

TElectionManager::TImpl::TImpl(
    TElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    NRpc::IServerPtr rpcServer)
    : TServiceBase(
        controlInvoker,
        TElectionServiceProxy::GetServiceName(),
        Logger.GetCategory())
        , State(EPeerState::Stopped)
        , VoteId(InvalidPeerId)
        , Config(config)
        , CellManager(cellManager)
        , ControlInvoker(controlInvoker)
        , ElectionCallbacks(electionCallbacks)
{
    YCHECK(cellManager);
    YCHECK(controlInvoker);
    YCHECK(electionCallbacks);
    YCHECK(rpcServer);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);

    Reset();

    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));

    rpcServer->RegisterService(this);
}

void TElectionManager::TImpl::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ControlInvoker->Invoke(BIND(&TThis::DoStart, MakeWeak(this)));
}

void TElectionManager::TImpl::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    // To prevent multiple restarts.
    auto epochContext = EpochContext;
    if (epochContext) {
        epochContext->CancelableContext->Cancel();
    }

    ControlInvoker->Invoke(BIND(&TThis::DoStop, MakeWeak(this)));
}

void TElectionManager::TImpl::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Restart forced");

    Stop();
    Start();
}

void TElectionManager::TImpl::GetMonitoringInfo(IYsonConsumer* consumer)
{
    auto epochContext = EpochContext;
    BuildYsonFluently(consumer)
        .BeginMap()
        .Item("state").Value(FormatEnum(State))
        .Item("peers").BeginList()
        .DoFor(0, CellManager->GetPeerCount(), [=] (TFluentList fluent, TPeerId id) {
            fluent.Item().Value(CellManager->GetPeerAddress(id));
    })
        .EndList()
        .DoIf(epochContext, [&] (TFluentMap fluent) {
            fluent
                .Item("leader_id").Value(epochContext->LeaderId)
                .Item("epoch_id").Value(epochContext->EpochId);
    })
        .Item("vote_id").Value(VoteId)
        .EndMap();
}

TEpochContextPtr TElectionManager::TImpl::GetEpochContext()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EpochContext;
}

void TElectionManager::TImpl::Reset()
{
    // May be called from ControlThread and also from ctor.

    SetState(EPeerState::Stopped);

    VoteId = InvalidPeerId;

    if (EpochContext) {
        EpochContext->CancelableContext->Cancel();
        EpochContext.Reset();
    }

    AliveFollowers.clear();
    PotentialFollowers.clear();
    PingTimeoutCookie.Reset();
}

void TElectionManager::TImpl::OnLeaderPingTimeout()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Following);

    LOG_INFO("No recurrent ping from leader within timeout");

    StopFollowing();
    StartVoting();
}

void TElectionManager::TImpl::DoStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Stopped);

    StartVoting();
}

void TElectionManager::TImpl::DoStop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State) {
    case EPeerState::Stopped:
        break;
    case EPeerState::Voting:
        Reset();
        break;
    case EPeerState::Leading:
        StopLeading();
        break;
    case EPeerState::Following:
        StopFollowing();
        break;
    default:
        YUNREACHABLE();
    }
}

void TElectionManager::TImpl::StartVoteFor(TPeerId voteId, const TEpochId& voteEpoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    State = EPeerState::Voting;
    VoteId = voteId;
    VoteEpochId = voteEpoch;

    TDelayedInvoker::Submit(
        BIND(&TThis::StartVotingRound, MakeStrong(this))
            .Via(ControlEpochInvoker),
        Config->VotingRoundInterval);
}

void TElectionManager::TImpl::StartVoting()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    State = EPeerState::Voting;
    VoteId = CellManager->GetSelfId();
    VoteEpochId = TGuid::Create();

    YCHECK(!EpochContext);
    EpochContext = New<TEpochContext>();
    ControlEpochInvoker = EpochContext->CancelableContext->CreateInvoker(ControlInvoker);

    auto priority = ElectionCallbacks->GetPriority();

    LOG_DEBUG("Voting for self (Priority: %s, VoteEpochId: %s)",
        ~ElectionCallbacks->FormatPriority(priority),
        ~ToString(VoteEpochId));

    StartVotingRound();
}

void TElectionManager::TImpl::StartVotingRound()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Voting);

    New<TVotingRound>(this)->Run();
}

void TElectionManager::TImpl::StartFollowing(
    TPeerId leaderId,
    const TEpochId& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Following);
    VoteId = leaderId;
    VoteEpochId = epoch;

    InitEpochContext(leaderId, epoch);

    PingTimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TThis::OnLeaderPingTimeout, MakeStrong(this))
            .Via(ControlEpochInvoker),
        Config->ReadyToFollowTimeout);

    LOG_INFO("Starting following leader (LeaderId: %d, EpochId: %s)",
        EpochContext->LeaderId,
        ~ToString(EpochContext->EpochId));

    ElectionCallbacks->OnStartFollowing();
}

void TElectionManager::TImpl::StartLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Leading);
    YCHECK(VoteId == CellManager->GetSelfId());

    // Initialize followers state.
    for (TPeerId i = 0; i < CellManager->GetPeerCount(); ++i) {
        AliveFollowers.insert(i);
        PotentialFollowers.insert(i);
    }

    InitEpochContext(CellManager->GetSelfId(), VoteEpochId);

    // Send initial pings.
    YCHECK(!FollowerPinger);
    FollowerPinger = New<TFollowerPinger>(this);
    FollowerPinger->Start();

    LOG_INFO("Starting leading (EpochId: %s)",
        ~ToString(EpochContext->EpochId));

    ElectionCallbacks->OnStartLeading();
}

void TElectionManager::TImpl::StopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Leading);

    LOG_INFO("Stopping leading (EpochId: %s)",
        ~ToString(EpochContext->EpochId));

    ElectionCallbacks->OnStopLeading();

    YCHECK(FollowerPinger);
    FollowerPinger->Stop();
    FollowerPinger.Reset();

    Reset();
}

void TElectionManager::TImpl::StopFollowing()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Following);

    LOG_INFO("Stopping following leader (LeaderId: %d, EpochId: %s)",
        EpochContext->LeaderId,
        ~ToString(EpochContext->EpochId));

    ElectionCallbacks->OnStopFollowing();

    Reset();
}

void TElectionManager::TImpl::InitEpochContext(TPeerId leaderId, const TEpochId& epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EpochContext->LeaderId = leaderId;
    EpochContext->EpochId = epochId;
    EpochContext->StartTime = TInstant::Now();
}

void TElectionManager::TImpl::SetState(EPeerState newState)
{
    if (newState == State)
        return;

    // This generic message logged to simplify tracking state changes.
    LOG_INFO("State changed: %s->%s",
        ~State.ToString(),
        ~newState.ToString());
    State = newState;
}

DEFINE_RPC_SERVICE_METHOD(TElectionManager::TImpl, PingFollower)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epochId = FromProto<TEpochId>(request->epoch_id());
    auto leaderId = request->leader_id();

    context->SetRequestInfo("Epoch: %s, LeaderId: %d",
        ~ToString(epochId),
        leaderId);

    if (State != EPeerState::Following) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::InvalidState,
            "Cannot process ping from a leader while in %s (LeaderId: %d, EpochId: %s)",
            ~State.ToString(),
            leaderId,
            ~ToString(epochId));
    }

    if (leaderId != EpochContext->LeaderId) {
        THROW_ERROR TError(
            EErrorCode::InvalidLeader,
            "Ping from an invalid leader: expected %d, received %d",
            EpochContext->LeaderId,
            leaderId);
    }

    if (epochId != EpochContext->EpochId) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::InvalidEpoch,
            "Ping with invalid epoch from leader: expected %s, received %s",
            ~ToString(EpochContext->EpochId),
            ~ToString(epochId));
    }

    TDelayedInvoker::Cancel(PingTimeoutCookie);

    PingTimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TThis::OnLeaderPingTimeout, MakeStrong(this))
            .Via(ControlEpochInvoker),
        Config->FollowerPingTimeout);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TElectionManager::TImpl, GetStatus)
{
    UNUSED(request);
    VERIFY_THREAD_AFFINITY(ControlThread);

    context->SetRequestInfo("");

    auto priority = ElectionCallbacks->GetPriority();

    response->set_state(State);
    response->set_vote_id(VoteId);
    response->set_priority(priority);
    ToProto(response->mutable_vote_epoch_id(), VoteEpochId);
    response->set_self_id(CellManager->GetSelfId());
    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        response->add_peer_addresses(CellManager->GetPeerAddress(id));
    }

    context->SetResponseInfo("State: %s, VoteId: %d, Priority: %s, VoteEpochId: %s",
        ~State.ToString(),
        VoteId,
        ~ElectionCallbacks->FormatPriority(priority),
        ~ToString(VoteEpochId));

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

TElectionManager::TElectionManager(
    TElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    NRpc::IServerPtr rpcServer)
    : Impl(New<TImpl>(
        config,
        cellManager,
        controlInvoker,
        electionCallbacks,
        rpcServer))
{ }

TElectionManager::~TElectionManager()
{ }

void TElectionManager::Start()
{
    Impl->Start();
}

void TElectionManager::Stop()
{
    Impl->Stop();
}

void TElectionManager::Restart()
{
    Impl->Restart();
}

void TElectionManager::GetMonitoringInfo(IYsonConsumer* consumer)
{
    Impl->GetMonitoringInfo(consumer);
}

TEpochContextPtr TElectionManager::GetEpochContext()
{
    return Impl->GetEpochContext();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
