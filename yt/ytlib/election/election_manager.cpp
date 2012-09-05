#include "election_manager.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/actions/parallel_awaiter.h>
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
    explicit TFollowerPinger(TElectionManagerPtr electionManager)
        : ElectionManager(electionManager)
        , Awaiter(New<TParallelAwaiter>(ElectionManager->ControlEpochInvoker))
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
    TElectionManagerPtr ElectionManager;
    TParallelAwaiterPtr Awaiter;

    void SendPing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        if (Awaiter->IsCanceled())
            return;

        LOG_DEBUG("Sending ping to follower %d", id);

        TProxy proxy(ElectionManager->CellManager->GetMasterChannel(id));
        auto request = proxy
            .PingFollower()
            ->SetTimeout(ElectionManager->Config->RpcTimeout);
        request->set_leader_id(ElectionManager->CellManager->GetSelfId());
        *request->mutable_epoch_id() = ElectionManager->EpochContext->EpochId.ToProto();

        Awaiter->Await(
            request->Invoke(),
            BIND(&TFollowerPinger::OnPingResponse, MakeStrong(this), id)
                .Via(ElectionManager->ControlEpochInvoker));
    }

    void SchedulePing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        TDelayedInvoker::Submit(
            BIND(&TFollowerPinger::SendPing, MakeStrong(this), id)
                .Via(ElectionManager->ControlEpochInvoker),
            ElectionManager->Config->FollowerPingInterval);
    }

    void OnPingResponse(TPeerId id, TProxy::TRspPingFollowerPtr response)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);
        YCHECK(ElectionManager->State == EPeerState::Leading);

        if (!response->IsOK()) {
            auto error = response->GetError();
            if (IsRpcError(error)) {
                // Hard error
                if (ElectionManager->AliveFollowers.erase(id) > 0) {
                    LOG_WARNING("Error pinging follower %d, considered down\n%s",
                        id,
                        ~ToString(error));
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
                            ~ToString(error));
                    }
                } else {
                    if (TInstant::Now() > ElectionManager->EpochContext->StartTime + ElectionManager->Config->PotentialFollowerTimeout) {
                        LOG_WARNING("Error pinging follower %d, no success within timeout, considered down\n%s",
                            id,
                            ~ToString(error));
                        ElectionManager->PotentialFollowers.erase(id);
                        ElectionManager->AliveFollowers.erase(id);
                    } else {
                        LOG_INFO("Error pinging follower %d, will retry later\n%s",
                            id,
                            ~ToString(error));
                    }
                }
            }

            if ((i32) ElectionManager->AliveFollowers.size() < ElectionManager->CellManager->GetQuorum()) {
                LOG_WARNING("Quorum is lost");
                ElectionManager->StopLeading();
                ElectionManager->StartVoting();
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
    explicit TVotingRound(TElectionManagerPtr electionManager)
        : ElectionManager(electionManager)
        , EpochInvoker(electionManager->ControlEpochInvoker)
        , Awaiter(New<TParallelAwaiter>(EpochInvoker))
    { }

    void Run() 
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);
        YCHECK(ElectionManager->State == EPeerState::Voting);

        auto callbacks = ElectionManager->ElectionCallbacks;
        auto cellManager = ElectionManager->CellManager;
        auto priority = callbacks->GetPriority();

        LOG_DEBUG("New voting round started (Round: %p, VoteId: %d, Priority: %s, VoteEpochId: %s)",
            this,
            ElectionManager->VoteId,
            ~callbacks->FormatPriority(priority),
            ~ElectionManager->VoteEpochId.ToString());

        ProcessVote(
            cellManager->GetSelfId(),
            TStatus(
                ElectionManager->State,
                ElectionManager->VoteId,
                priority,
                ElectionManager->VoteEpochId));

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            TProxy proxy(cellManager->GetMasterChannel(id));
            proxy.SetDefaultTimeout(ElectionManager->Config->RpcTimeout);
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

    TElectionManagerPtr ElectionManager;
    IInvokerPtr EpochInvoker;
    TParallelAwaiterPtr Awaiter;
    TStatusTable StatusTable;

    bool ProcessVote(TPeerId id, const TStatus& status)
    {
        StatusTable[id] = status;
        return CheckForLeader();
    }

    void OnResponse(TPeerId id, TProxy::TRspGetStatusPtr response)
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        if (!response->IsOK()) {
            LOG_INFO("Error requesting status from peer %d (Round: %p)\n%s",
                id,
                this,
                ~ToString(response->GetError()));
            return;
        }

        auto state = EPeerState(response->state());
        auto vote = response->vote_id();
        auto priority = response->priority();
        auto epochId = TEpochId::FromProto(response->vote_epoch_id());
        
        LOG_DEBUG("Received status from peer %d (Round: %p, State: %s, VoteId: %d, Priority: %s, VoteEpochId: %s)",
            id,
            this,
            ~state.ToString(),
            vote,
            ~ElectionManager->ElectionCallbacks->FormatPriority(priority),
            ~epochId.ToString());

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
            candidateId == ElectionManager->CellManager->GetSelfId()
            ? ElectionManager->VoteEpochId
            : candidateStatus.VoteEpochId;

        // Count votes (including self) and quorum.
        int voteCount = CountVotesFor(candidateId, candidateEpochId);
        int quorum = ElectionManager->CellManager->GetQuorum();
        
        // Check for quorum.
        if (voteCount < quorum) {
            LOG_DEBUG("Candidate %d has too few votes: %d < %d (Round: %p, VoteEpochId: %s)",
                candidateId,
                voteCount,
                quorum,
                this,
                ~candidateEpochId.ToString());
            return false;
        }

        LOG_DEBUG("Candidate %d has quorum: %d >= %d (Round: %p, VoteEpochId: %s)",
            candidateId,
            voteCount,
            quorum,
            this,
            ~candidateEpochId.ToString());

        Awaiter->Cancel();

        // Become a leader or a follower.
        if (candidateId == ElectionManager->CellManager->GetSelfId()) {
            EpochInvoker->Invoke(BIND(
                &TElectionManager::StartLeading,
                ElectionManager));
        } else {
            EpochInvoker->Invoke(BIND(
                &TElectionManager::StartFollowing,
                ElectionManager,
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

        if (candidateId == ElectionManager->CellManager->GetSelfId()) {
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
        ElectionManager->StartVoteFor(candidateStatus.VoteId, candidateStatus.VoteEpochId);
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY(ElectionManager->ControlThread);

        LOG_DEBUG("Voting round completed (Round: %p)", this);

        ChooseVote();
    }
};

////////////////////////////////////////////////////////////////////////////////

TElectionManager::TElectionManager(
    TElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks)
    : TServiceBase(
    controlInvoker,
    TProxy::GetServiceName(),
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

    // To prevent multiple restarts.
    auto epochContext = EpochContext;
    if (epochContext) {
        epochContext->CancelableContext->Cancel();
    }

    ControlInvoker->Invoke(BIND(&TElectionManager::DoStop, MakeWeak(this)));
}

void TElectionManager::Restart()
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_INFO("Restart forced");

    Stop();
    Start();
}

void TElectionManager::GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
{
    auto epochContext = EpochContext;
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("state").Scalar(FormatEnum(State))
            .Item("peers").BeginList()
                .DoFor(0, CellManager->GetPeerCount(), [=] (TFluentList fluent, TPeerId id) {
                    fluent.Item().Scalar(CellManager->GetPeerAddress(id));
                })
            .EndList()
            .DoIf(epochContext, [&] (TFluentMap fluent) {
                fluent
                    .Item("leader_id").Scalar(epochContext->LeaderId)
                    .Item("epoch_id").Scalar(epochContext->EpochId);
            })
            .Item("vote_id").Scalar(VoteId)
        .EndMap();
}

TEpochContextPtr TElectionManager::GetEpochContext()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EpochContext;
}

void TElectionManager::Reset()
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

void TElectionManager::OnLeaderPingTimeout()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Following);
    
    LOG_INFO("No recurrent ping from leader within timeout");
    
    StopFollowing();
    StartVoting();
}

void TElectionManager::DoStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Stopped);

    StartVoting();
}

void TElectionManager::DoStop()
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

void TElectionManager::StartVoteFor(TPeerId voteId, const TEpochId& voteEpoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    State = EPeerState::Voting;
    VoteId = voteId;
    VoteEpochId = voteEpoch;

    TDelayedInvoker::Submit(
        BIND(&TElectionManager::StartVotingRound, MakeStrong(this))
            .Via(ControlInvoker),
        Config->VotingRoundInterval);
}

void TElectionManager::StartVoting()
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
        ~VoteEpochId.ToString());

    StartVotingRound();
}

void TElectionManager::StartVotingRound()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Voting);

    New<TVotingRound>(this)->Run();
}

void TElectionManager::StartFollowing(
    TPeerId leaderId,
    const TEpochId& epoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Following);
    VoteId = leaderId;
    VoteEpochId = epoch;

    InitEpochContext(leaderId, epoch);

    PingTimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TElectionManager::OnLeaderPingTimeout, MakeStrong(this))
            .Via(ControlEpochInvoker),
        Config->ReadyToFollowTimeout);

    LOG_INFO("Starting following leader (LeaderId: %d, EpochId: %s)",
        EpochContext->LeaderId,
        ~EpochContext->EpochId.ToString());

    ElectionCallbacks->OnStartFollowing();
}

void TElectionManager::StartLeading()
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
        ~EpochContext->EpochId.ToString());
    
    ElectionCallbacks->OnStartLeading();
}

void TElectionManager::StopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Leading);
    
    LOG_INFO("Stopping leading (EpochId: %s)",
        ~EpochContext->EpochId.ToString());

    ElectionCallbacks->OnStopLeading();

    YCHECK(FollowerPinger);
    FollowerPinger->Stop();
    FollowerPinger.Reset();

    Reset();
}

void TElectionManager::StopFollowing()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Following);

    LOG_INFO("Stopping following leader (LeaderId: %d, EpochId: %s)",
        EpochContext->LeaderId,
        ~EpochContext->EpochId.ToString());
        
    ElectionCallbacks->OnStopFollowing();
    
    Reset();
}

void TElectionManager::InitEpochContext(TPeerId leaderId, const TEpochId& epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EpochContext->LeaderId = leaderId;
    EpochContext->EpochId = epochId;
    EpochContext->StartTime = TInstant::Now();
}

void TElectionManager::SetState(EPeerState newState)
{
    if (newState == State)
        return;

    // This generic message logged to simplify tracking state changes.
    LOG_INFO("State changed: %s->%s",
        ~State.ToString(),
        ~newState.ToString());
    State = newState;
}

DEFINE_RPC_SERVICE_METHOD(TElectionManager, PingFollower)
{
    UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epochId = TEpochId::FromProto(request->epoch_id());
    auto leaderId = request->leader_id();

    context->SetRequestInfo("Epoch: %s, LeaderId: %d",
        ~epochId.ToString(),
        leaderId);

    if (State != EPeerState::Following) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::InvalidState,
            "Cannot process ping from a leader while in %s (LeaderId: %d, EpochId: %s)",
            ~State.ToString(),
            leaderId,
            ~epochId.ToString());
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
            ~EpochContext->EpochId.ToString(),
            ~epochId.ToString());
    }

    TDelayedInvoker::Cancel(PingTimeoutCookie);

    PingTimeoutCookie = TDelayedInvoker::Submit(
        BIND(&TElectionManager::OnLeaderPingTimeout, MakeStrong(this))
            .Via(ControlEpochInvoker),
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
    *response->mutable_vote_epoch_id() = VoteEpochId.ToProto();
    response->set_self_id(CellManager->GetSelfId());
    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        response->add_peer_addresses(CellManager->GetPeerAddress(id));
    }

    context->SetResponseInfo("State: %s, VoteId: %d, Priority: %s, VoteEpochId: %s",
        ~State.ToString(),
        VoteId,
        ~ElectionCallbacks->FormatPriority(priority),
        ~VoteEpochId.ToString());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
