
#include "election_manager.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/logging/log.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/profiling/profiler.h>

namespace NYT {
namespace NElection {

using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = ElectionLogger;
static NProfiling::TProfiler Profiler("/election");

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TFollowerPinger
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TFollowerPinger> TPtr;

    TFollowerPinger(TElectionManager::TPtr electionManager)
        : ElectionManager(electionManager)
        , EpochInvoker(~electionManager->ControlEpochInvoker)
        , Awaiter(New<TParallelAwaiter>(~EpochInvoker))
    { }

    void Start()
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        auto& cellManager = ElectionManager->CellManager;
        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id != cellManager->GetSelfId()) {
                SendPing(id);
            }
        }
    }

    void Stop()
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        // Do nothing.
    }

private:
    TElectionManager::TPtr ElectionManager;
    IInvoker::TPtr EpochInvoker;
    TParallelAwaiter::TPtr Awaiter;

    void SendPing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        if (Awaiter->IsCanceled())
            return;

        LOG_DEBUG("Sending ping to follower %d", id);

        auto proxy = ElectionManager
            ->CellManager
            ->GetMasterProxy<TProxy>(id);
        auto request = proxy
            ->PingFollower()
            ->SetTimeout(ElectionManager->Config->RpcTimeout);
        request->set_leader_id(ElectionManager->CellManager->GetSelfId());
        *request->mutable_epoch() = ElectionManager->Epoch.ToProto();
        Awaiter->Await(
            request->Invoke(),
            BIND(&TFollowerPinger::OnPingResponse, MakeStrong(this), id)
            .Via(EpochInvoker));
    }

    void SchedulePing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        TDelayedInvoker::Submit(
            BIND(&TFollowerPinger::SendPing, MakeStrong(this), id)
            .Via(EpochInvoker),
            ElectionManager->Config->FollowerPingInterval);
    }

    void OnPingResponse(TPeerId id, TProxy::TRspPingFollower::TPtr response)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);
        YASSERT(ElectionManager->State == TProxy::EState::Leading);

        if (!response->IsOK()) {
            auto error = response->GetError();
            if (IsRpcError(error)) {
                // Hard error
                if (ElectionManager->AliveFollowers.erase(id) > 0) {
                    LOG_WARNING("Error pinging follower %d, considered down\n%s",
                        id,
                        ~error.ToString());
                    ElectionManager->PotentialFollowers.erase(id);
                }
            } else {
                // Soft error
                if (ElectionManager->PotentialFollowers.find(id) ==
                    ElectionManager->PotentialFollowers.end())
                {
                    if (ElectionManager->AliveFollowers.erase(id) > 0) {
                        LOG_WARNING("Error pinging follower %d, considered down\n%s",
                            id,
                            ~error.ToString());
                    }
                } else {
                    if (TInstant::Now() > ElectionManager->EpochStart + ElectionManager->Config->PotentialFollowerTimeout) {
                        LOG_WARNING("Error pinging follower %d, no success within timeout, considered down\n%s",
                            id,
                            ~error.ToString());
                        ElectionManager->PotentialFollowers.erase(id);
                        ElectionManager->AliveFollowers.erase(id);
                    } else {
                        LOG_INFO("Error pinging follower %d, will retry later\n%s",
                            id,
                            ~error.ToString());
                    }
                }
            }

            if ((i32) ElectionManager->AliveFollowers.size() < ElectionManager->CellManager->GetQuorum()) {
                LOG_WARNING("Quorum is lost");
                ElectionManager->StopLeading();
                ElectionManager->StartVoteForSelf();
                return;
            }
            
            if (response->GetErrorCode() == NRpc::EErrorCode::Timeout) {
                SendPing(id);
            } else {
                SchedulePing(id);
            }

            return;
        }

        LOG_DEBUG("Ping reply received from follower %d", id);

        if (ElectionManager->PotentialFollowers.find(id) !=
            ElectionManager->PotentialFollowers.end())
        {
            LOG_INFO("Follower %d is up, first success", id);
            ElectionManager->PotentialFollowers.erase(id);
        }
        else if (ElectionManager->AliveFollowers.find(id) ==
                 ElectionManager->AliveFollowers.end())
        {
            LOG_INFO("Follower %d is up", id);
            ElectionManager->AliveFollowers.insert(id);
        }

        SchedulePing(id);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TVotingRound
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TVotingRound> TPtr;

    TVotingRound(TElectionManager::TPtr electionManager)
        : ElectionManager(electionManager)
        , EpochInvoker(~electionManager->ControlEpochInvoker)
        , Awaiter(New<TParallelAwaiter>(~EpochInvoker))
    { }

    void Run() 
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);
        YASSERT(ElectionManager->State == TProxy::EState::Voting);

        auto callbacks = ElectionManager->ElectionCallbacks;
        auto cellManager = ElectionManager->CellManager;
        auto priority = callbacks->GetPriority();

        LOG_DEBUG("New voting round started (Round: %p, VoteId: %d, Priority: %s, VoteEpoch: %s)",
            this,
            ElectionManager->VoteId,
            ~callbacks->FormatPriority(priority),
            ~ElectionManager->VoteEpoch.ToString());

        ProcessVote(
            cellManager->GetSelfId(),
            TStatus(
                ElectionManager->State,
                ElectionManager->VoteId,
                priority,
                ElectionManager->VoteEpoch));

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            auto proxy = cellManager->GetMasterProxy<TProxy>(id);
            proxy->SetDefaultTimeout(ElectionManager->Config->RpcTimeout);

            auto request = proxy->GetStatus();
            Awaiter->Await(
                request->Invoke(),
                BIND(&TThis::OnResponse, MakeStrong(this), id));
        }

        Awaiter->Complete(BIND(&TThis::OnComplete, MakeStrong(this)));
    }

private:
    typedef TVotingRound TThis;

    struct TStatus
    {
        TProxy::EState State;
        TPeerId VoteId;
        TPeerPriority Priority;
        TEpoch VoteEpoch;

        TStatus(
            TProxy::EState state = TProxy::EState::Stopped,
            TPeerId vote = InvalidPeerId,
            TPeerPriority priority = -1,
            TEpoch epoch = TEpoch())
            : State(state)
            , VoteId(vote)
            , Priority(priority)
            , VoteEpoch(epoch)
        { }
    };

    typedef yhash_map<TPeerId, TStatus> TStatusTable;

    TElectionManager::TPtr ElectionManager;
    IInvoker::TPtr EpochInvoker;
    TParallelAwaiter::TPtr Awaiter;
    TStatusTable StatusTable;

    bool ProcessVote(TPeerId id, const TStatus& status)
    {
        StatusTable[id] = status;
        return CheckForLeader();
    }

    void OnResponse(TPeerId id, TProxy::TRspGetStatus::TPtr response)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        if (!response->IsOK()) {
            LOG_INFO("Error requesting status from peer %d (Round: %p)\n%s",
                id,
                this,
                ~response->GetError().ToString());
            return;
        }

        auto state = TProxy::EState(response->state());
        auto vote = response->vote_id();
        auto priority = response->priority();
        auto epoch = TEpoch::FromProto(response->vote_epoch());
        
        LOG_DEBUG("Received status from peer %d (Round: %p, State: %s, VoteId: %d, Priority: %s, VoteEpoch: %s)",
            id,
            this,
            ~state.ToString(),
            vote,
            ~ElectionManager->ElectionCallbacks->FormatPriority(priority),
            ~epoch.ToString());

        ProcessVote(id, TStatus(state, vote, priority, epoch));
    }

    bool CheckForLeader()
    {
        LOG_DEBUG("Checking candidates (Round: %p)", this);

        FOREACH(const auto& pair, StatusTable) {
            if (CheckForLeader(pair.first, pair.second))
                return true;
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
        auto candidateEpoch =
            candidateId == ElectionManager->CellManager->GetSelfId()
            ? ElectionManager->VoteEpoch
            : candidateStatus.VoteEpoch;

        // Count votes (including self) and quorum.
        int voteCount = CountVotes(candidateId, candidateEpoch);
        int quorum = ElectionManager->CellManager->GetQuorum();
        
        // Check for quorum.
        if (voteCount < quorum) {
            LOG_DEBUG("Candidate %d has too few votes (Round: %p, VoteEpoch: %s, VoteCount: %d, Quorum: %d)",
                candidateId,
                this,
                ~candidateEpoch.ToString(),
                voteCount,
                quorum);
            return false;
        }

        LOG_DEBUG("Candidate %d has quorum (Round: %p, VoteEpoch: %s, VoteCount: %d, Quorum: %d)",
            candidateId,
            this,
            ~candidateEpoch.ToString(),
            voteCount,
            quorum);

        Awaiter->Cancel();

        // Become a leader or a follower.
        if (candidateId == ElectionManager->CellManager->GetSelfId()) {
            EpochInvoker->Invoke(BIND(
                &TElectionManager::StartLeading,
                TElectionManager::TPtr(ElectionManager)));
        } else {
            EpochInvoker->Invoke(BIND(
                &TElectionManager::StartFollowing,
                TElectionManager::TPtr(ElectionManager),
                candidateId,
                candidateStatus.VoteEpoch));
        }

        return true;
    }

    int CountVotes(
        TPeerId candidateId,
        const TEpoch& epoch) const
    {
        int count = 0;
        FOREACH(const auto& pair, StatusTable) {
            if (pair.second.VoteId == candidateId &&
                pair.second.VoteEpoch == epoch)
            {
                ++count;
            }
        }
        return count;
    }

    bool IsFeasibleCandidate(
        TPeerId candidateId,
        const TStatus& candidateStatus) const
    {
        // He must be voting for himself.
        if (candidateId != candidateStatus.VoteId)
            return false;

        if (candidateId == ElectionManager->CellManager->GetSelfId()) {
            // Check that we're voting.
            YASSERT(candidateStatus.State == TProxy::EState::Voting);
            return true;
        } else {
            // The candidate must be aware of his leadership.
            return candidateStatus.State == TProxy::EState::Leading;
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
        FOREACH(const auto& pair, StatusTable) {
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
        ElectionManager->StartVoteFor(candidateStatus.VoteId, candidateStatus.VoteEpoch);
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        LOG_DEBUG("Voting round completed (Round: %p)",
            this);

        ChooseVote();
    }
};

////////////////////////////////////////////////////////////////////////////////

TElectionManager::TElectionManager(
    TElectionManagerConfig *config,
    TCellManager* cellManager,
    IInvoker* controlInvoker,
    IElectionCallbacks* electionCallbacks)
    : TServiceBase(
    controlInvoker,
    TProxy::GetServiceName(),
    Logger.GetCategory())
    , State(TProxy::EState::Stopped)
    , VoteId(InvalidPeerId)
    , Config(config)
    , CellManager(cellManager)
    , ControlInvoker(controlInvoker)
    , ElectionCallbacks(electionCallbacks)
{
    YASSERT(cellManager);
    YASSERT(controlInvoker);
    YASSERT(electionCallbacks);

    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);

    Reset();

    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));
}

TElectionManager::~TElectionManager()
{ }

void TElectionManager::Start()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ControlInvoker->Invoke(BIND(&TElectionManager::DoStart, MakeWeak(this)));
}

void TElectionManager::Stop()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ControlInvoker->Invoke(BIND(&TElectionManager::DoStop, MakeWeak(this)));
}

void TElectionManager::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Restart forced");

    Stop();
    Start();
}

void TElectionManager::Reset()
{
    // May be called from ControlThread and also from ctor.

    UpdateState(TProxy::EState::Stopped);
    VoteId = InvalidPeerId;
    LeaderId = InvalidPeerId;
    VoteEpoch = TGuid();
    Epoch = TGuid();
    EpochStart = TInstant();
    if (ControlEpochContext) {
        ControlEpochContext->Cancel();
        ControlEpochContext.Reset();
        ControlEpochInvoker.Reset();
    }
    AliveFollowers.clear();
    PotentialFollowers.clear();
    PingTimeoutCookie.Reset();
}

void TElectionManager::OnLeaderPingTimeout()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(State == TProxy::EState::Following);
    
    LOG_INFO("No recurrent ping from leader within timeout");
    
    StopFollowing();
    StartVoteForSelf();
}

void TElectionManager::DoStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(State == TProxy::EState::Stopped);

    StartVoteForSelf();
}

void TElectionManager::DoStop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State) {
        case TProxy::EState::Stopped:
            break;
        case TProxy::EState::Voting:
            Reset();
            break;            
        case TProxy::EState::Leading:
            StopLeading();
            break;
        case TProxy::EState::Following:
            StopFollowing();
            break;
        default:
            YUNREACHABLE();
    }
}

void TElectionManager::StartVoteFor(TPeerId voteId, const TEpoch& voteEpoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    State = TProxy::EState::Voting;
    VoteId = voteId;
    VoteEpoch = voteEpoch;
    StartVotingRound();
}

void TElectionManager::StartVoteForSelf()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    State = TProxy::EState::Voting;
    VoteId = CellManager->GetSelfId();
    VoteEpoch = TGuid::Create();

    YASSERT(!ControlEpochContext);
    YASSERT(!ControlEpochInvoker);
    ControlEpochContext = New<TCancelableContext>();
    ControlEpochInvoker = ControlEpochContext->CreateInvoker(~ControlInvoker);

    auto priority = ElectionCallbacks->GetPriority();

    LOG_DEBUG("Voting for self (Priority: %s, VoteEpoch: %s)",
        ~ElectionCallbacks->FormatPriority(priority),
        ~VoteEpoch.ToString());

    StartVotingRound();
}

void TElectionManager::StartVotingRound()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(State == TProxy::EState::Voting);

    New<TVotingRound>(this)->Run();
}

void TElectionManager::StartFollowing(
    TPeerId leaderId,
    const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UpdateState(TProxy::EState::Following);
    VoteId = leaderId;
    VoteEpoch = epoch;

    StartEpoch(leaderId, epoch);

    // XXX(sandello): Capture policy here?
    PingTimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TElectionManager::OnLeaderPingTimeout, MakeStrong(this))
        .Via(~ControlEpochInvoker),
        Config->ReadyToFollowTimeout);

    LOG_INFO("Starting following leader %d (Epoch: %s)",
        LeaderId,
        ~Epoch.ToString());

    ElectionCallbacks->OnStartFollowing(LeaderId, Epoch);
}

void TElectionManager::StartLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    UpdateState(TProxy::EState::Leading);
    YASSERT(VoteId == CellManager->GetSelfId());

    // Initialize followers state.
    for (TPeerId i = 0; i < CellManager->GetPeerCount(); ++i) {
        AliveFollowers.insert(i);
        PotentialFollowers.insert(i);
    }
    
    StartEpoch(CellManager->GetSelfId(), VoteEpoch);

    // Send initial pings.
    YASSERT(!FollowerPinger);
    FollowerPinger = New<TFollowerPinger>(this);
    FollowerPinger->Start();

    LOG_INFO("Starting leading (Epoch: %s)", ~Epoch.ToString());
    
    ElectionCallbacks->OnStartLeading(Epoch);
}

void TElectionManager::StopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(State == TProxy::EState::Leading);
    
    LOG_INFO("Stopping leading (Epoch: %s)",
        ~Epoch.ToString());

    ElectionCallbacks->OnStopLeading();

    YASSERT(FollowerPinger);
    FollowerPinger->Stop();
    FollowerPinger.Reset();

    StopEpoch();
    
    Reset();
}

void TElectionManager::StopFollowing()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YASSERT(State == TProxy::EState::Following);

    LOG_INFO("Stopping following leader %d (Epoch: %s)",
        LeaderId,
        ~Epoch.ToString());
        
    ElectionCallbacks->OnStopFollowing();
    
    StopEpoch();
    
    Reset();
}

void TElectionManager::StartEpoch(TPeerId leaderId, const TEpoch& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LeaderId = leaderId;
    Epoch = epoch;
    EpochStart = Now();
}

void TElectionManager::StopEpoch()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LeaderId = InvalidPeerId;
    Epoch = TGuid();
    EpochStart = TInstant();
}

void TElectionManager::GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
{
    auto current = BuildYsonFluently(consumer)
        .BeginMap()
            .Item("state").Scalar(State.ToString())
            .Item("peers").BeginList()
                .DoFor(0, CellManager->GetPeerCount(), [=] (TFluentList fluent, TPeerId id) {
                    fluent.Item().Scalar(CellManager->GetPeerAddress(id));
                })
            .EndList()
            .Item("leader_id").Scalar(LeaderId)
            .Item("vote_id").Scalar(VoteId)
        .EndMap();
}

void TElectionManager::UpdateState(TProxy::EState newState)
{
    if (newState == State)
        return;

    // This generic message logged to simplify tracking state changes.
    LOG_INFO("State changed from %s to %s",
        ~State.ToString(),
        ~newState.ToString());
    State = newState;
}

DEFINE_RPC_SERVICE_METHOD(TElectionManager, PingFollower)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epoch = TEpoch::FromProto(request->epoch());
    auto leaderId = request->leader_id();

    context->SetRequestInfo("Epoch: %s, LeaderId: %d",
        ~epoch.ToString(),
        leaderId);

    if (State != TProxy::EState::Following) {
        ythrow TServiceException(EErrorCode::InvalidState) <<
            Sprintf("Cannot process ping from a leader while in %s (LeaderId: %d, Epoch: %s)",
            ~State.ToString(),
            leaderId,
            ~epoch.ToString());
    }

    if (leaderId != LeaderId) {
        ythrow TServiceException(EErrorCode::InvalidLeader) <<
            Sprintf("Ping from an invalid leader: expected %d, received %d",
            LeaderId,
            leaderId);
    }

    if (epoch != Epoch) {
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
            Sprintf("Ping with invalid epoch from leader: expected %s, received %s",
            ~Epoch.ToString(),
            ~epoch.ToString());
    }

    TDelayedInvoker::Cancel(PingTimeoutCookie);
    // XXX(sandello): Capture policy here?
    PingTimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TElectionManager::OnLeaderPingTimeout, MakeStrong(this))
        .Via(~ControlEpochInvoker),
        Config->FollowerPingTimeout);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TElectionManager, GetStatus)
{
    UNUSED(request);
    VERIFY_THREAD_AFFINITY(ControlThread);

    context->SetRequestInfo("");

    auto priority = ElectionCallbacks->GetPriority();

    response->set_state(State);
    response->set_vote_id(VoteId);
    response->set_priority(priority);
    *response->mutable_vote_epoch() = VoteEpoch.ToProto();
    response->set_self_id(CellManager->GetSelfId());
    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        response->add_peer_addresses(CellManager->GetPeerAddress(id));
    }

    context->SetResponseInfo("State: %s, VoteId: %d, Priority: %s, VoteEpoch: %s",
        ~State.ToString(),
        VoteId,
        ~ElectionCallbacks->FormatPriority(priority),
        ~VoteEpoch.ToString());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
