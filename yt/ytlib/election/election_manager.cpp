#include "election_manager.h"
#include "leader_lookup.h"

#include "../misc/string.h"
#include "../misc/serialize.h"
#include "../logging/log.h"
#include "../actions/action_util.h"

#include <util/folder/dirut.h>

namespace NYT {

using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Election");

////////////////////////////////////////////////////////////////////////////////

TElectionManager::TConfig::TConfig()
{ }

// TODO: refactor
const int Multiplier = 1000;

const TDuration TElectionManager::TConfig::RpcTimeout = TDuration::MilliSeconds(1 * Multiplier);

const TDuration TElectionManager::TConfig::FollowerPingInterval = TDuration::MilliSeconds(1 * Multiplier);
const TDuration TElectionManager::TConfig::FollowerPingTimeout = TDuration::MilliSeconds(5 * Multiplier);

const TDuration TElectionManager::TConfig::ReadyToFollowTimeout = TDuration::MilliSeconds(5 * Multiplier);
const TDuration TElectionManager::TConfig::PotentialFollowerTimeout = TDuration::MilliSeconds(5 * Multiplier);

////////////////////////////////////////////////////////////////////////////////

TElectionManager::TElectionManager(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr invoker,
    IElectionCallbacks::TPtr electionCallbacks,
    NRpc::TServer::TPtr server)
    : TServiceBase(TProxy::GetServiceName(), Logger.GetCategory())
    , State(TProxy::EState::Stopped)
    , VoteId(InvalidMasterId)
    , Config(config)
    , CellManager(cellManager)
    , Invoker(invoker)
    , ElectionCallbacks(electionCallbacks)
{
    Reset();

    RegisterMethods();
    
    server->RegisterService(this);
}

TElectionManager::~TElectionManager()
{ }

void TElectionManager::RegisterMethods()
{
    RPC_REGISTER_METHOD(TElectionManager, PingFollower);
    RPC_REGISTER_METHOD(TElectionManager, GetStatus);
}

void TElectionManager::Start()
{
    Invoker->Invoke(FromMethod(&TElectionManager::DoStart, this));
}

void TElectionManager::Stop()
{
    Invoker->Invoke(FromMethod(&TElectionManager::DoStop, this));
}

void TElectionManager::Restart()
{
    Stop();
    Start();
}

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TFollowerPinger
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFollowerPinger> TPtr;

    TFollowerPinger(TElectionManager* electionManager)
        : ElectionManager(electionManager)
        , Awaiter(new TParallelAwaiter(electionManager->Invoker))
    { }

    void Run()
    {
        TCellManager::TPtr cellManager = ElectionManager->CellManager;
        for (TMasterId i = 0; i < cellManager->GetMasterCount(); ++i) {
            if (i == cellManager->GetSelfId()) continue;
            SendPing(i);
        }
    }

    void Cancel()
    {
        Awaiter->Cancel();
    }

private:
    TElectionManager* ElectionManager;
    TParallelAwaiter::TPtr Awaiter;

    void SendPing(TMasterId id)
    {
        if (Awaiter->IsCanceled())
            return;

        LOG_DEBUG("Sending ping to follower %d", id);

        THolder<TProxy> proxy(ElectionManager->CellManager->GetMasterProxy<TProxy>(id));
        TProxy::TReqPingFollower::TPtr request = proxy->PingFollower();
        request->SetLeaderId(ElectionManager->CellManager->GetSelfId());
        request->SetEpoch(ProtoGuidFromGuid(ElectionManager->Epoch));
        Awaiter->Await(
            request->Invoke(TConfig::RpcTimeout),
            FromMethod(&TFollowerPinger::OnResponse, TPtr(this), id));
    }

    void SchedulePing(TMasterId id)
    {
        TDelayedInvoker::Get()->Submit(
            FromMethod(&TFollowerPinger::SendPing, TPtr(this), id)
            ->Via(~ElectionManager->EpochInvoker)
            ->Via(ElectionManager->Invoker),
            TConfig::FollowerPingInterval);
    }

    void OnResponse(TProxy::TRspPingFollower::TPtr response, TMasterId id)
    {
        YASSERT(ElectionManager->State == TProxy::EState::Leading);

        if (!response->IsOK()) {
            TProxy::EErrorCode errorCode = response->GetErrorCode();
            if (response->IsRpcError()) {
                // Hard error
                if (ElectionManager->AliveFollowers.erase(id) > 0) {
                    LOG_WARNING("Error pinging follower %d, considered down (ErrorCode: %s)",
                        id,
                        ~errorCode.ToString());
                    ElectionManager->PotentialFollowers.erase(id);
                }
            } else {
                // Soft error
                if (ElectionManager->PotentialFollowers.find(id) ==
                    ElectionManager->PotentialFollowers.end())
                {
                    if (ElectionManager->AliveFollowers.erase(id) > 0) {
                        LOG_WARNING("Error pinging follower %d, considered down (ErrorCode: %s)",
                            id,
                            ~errorCode.ToString());
                    }
                } else {
                    if (TInstant::Now() > ElectionManager->EpochStart + TConfig::PotentialFollowerTimeout) {
                        LOG_WARNING("Error pinging follower %d, no success within timeout, considered down (ErrorCode: %s)",
                            id,
                            ~errorCode.ToString());
                        ElectionManager->PotentialFollowers.erase(id);
                        ElectionManager->AliveFollowers.erase(id);
                    } else {
                        LOG_INFO("Error pinging follower %d, will retry later (ErrorCode: %s)",
                            id,
                            ~errorCode.ToString());
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

        LOG_DEBUG("Ping reply from follower %d", id);

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
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TVotingRound> TPtr;

    TVotingRound(TElectionManager::TPtr electionManager)
        : ElectionManager(electionManager)
        , Awaiter(new TParallelAwaiter(electionManager->Invoker))
        , EpochInvoker(electionManager->EpochInvoker)
    { }

    void Run() 
    {
        YASSERT(ElectionManager->State == TProxy::EState::Voting);

        IElectionCallbacks::TPtr callbacks = ElectionManager->ElectionCallbacks;
        TCellManager::TPtr cellManager = ElectionManager->CellManager;
        
        TMasterPriority priority = callbacks->GetPriority();

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

        for (TMasterId id = 0; id < cellManager->GetMasterCount(); ++id) {
            if (id == cellManager->GetSelfId()) continue;

            THolder<TProxy> proxy(cellManager->GetMasterProxy<TProxy>(id));
            TProxy::TReqGetStatus::TPtr request = proxy->GetStatus();
            Awaiter->Await(
                request->Invoke(TConfig::RpcTimeout),
                FromMethod(&TVotingRound::OnResponse, TPtr(this), id));
        }

        Awaiter->Complete(FromMethod(&TVotingRound::OnComplete, TPtr(this)));
    }

private:
    struct TStatus
    {
        TProxy::EState State;
        TMasterId VoteId;
        TMasterPriority Priority;
        TMasterEpoch VoteEpoch;

        TStatus(
            TProxy::EState state = TProxy::EState::Stopped,
            TMasterId vote = InvalidMasterId,
            TMasterPriority priority = -1,
            TMasterEpoch epoch = TMasterEpoch())
            : State(state)
            , VoteId(vote)
            , Priority(priority)
            , VoteEpoch(epoch)
        { }
    };

    typedef yhash_map<TMasterId, TStatus> TStatusTable;

    TElectionManager::TPtr ElectionManager;
    TParallelAwaiter::TPtr Awaiter;
    TCancelableInvoker::TPtr EpochInvoker;
    TStatusTable StatusTable;

    bool ProcessVote(TMasterId id, const TStatus& status)
    {
        StatusTable[id] = status;
        return CheckForLeader();
    }

    void OnResponse(TProxy::TRspGetStatus::TPtr response, TMasterId masterId)
    {
        if (!response->IsOK()) {
            LOG_INFO("Error requesting status from master %d (Round: %p, ErrorCode: %s)",
                       masterId,
                       this,
                       ~response->GetErrorCode().ToString());
            return;
        }

        TProxy::EState state = TProxy::EState(response->GetState());
        TMasterId vote = response->GetVoteId();
        TMasterPriority priority = response->GetPriority();
        TMasterEpoch epoch = GuidFromProtoGuid(response->GetVoteEpoch());
        
        LOG_DEBUG("Received status from master %d (Round: %p, State: %s, VoteId: %d, Priority: %s, VoteEpoch: %s)",
            masterId,
            this,
            ~state.ToString(),
            vote,
            ~ElectionManager->ElectionCallbacks->FormatPriority(priority),
            ~epoch.ToString());

        ProcessVote(masterId, TStatus(state, vote, priority, epoch));
    }

    bool CheckForLeader()
    {
        LOG_DEBUG("Checking candidates (Round: %p)", this);

        for (TStatusTable::iterator it = StatusTable.begin();
             it != StatusTable.end();
             ++it)
        {
            if (CheckForLeader(it->First(), it->Second()))
                return true;
        }

        LOG_DEBUG("No leader candidate found (Round: %p)", this);

        return false;
    }

    bool CheckForLeader(
        TMasterId candidateId,
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
        TMasterEpoch candidateEpoch =
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
            EpochInvoker->Invoke(FromMethod(
                &TElectionManager::StartLeading,
                TElectionManager::TPtr(ElectionManager)));
        } else {
            EpochInvoker->Invoke(FromMethod(
                &TElectionManager::StartFollowing,
                TElectionManager::TPtr(ElectionManager),
                candidateId,
                candidateStatus.VoteEpoch));
        }

        return true;
    }

    int CountVotes(
        TMasterId candidateId,
        const TMasterEpoch& epoch) const
    {
        int count = 0;
        for (TStatusTable::const_iterator it = StatusTable.begin();
             it != StatusTable.end();
             ++it)
        {
            if (it->Second().VoteId == candidateId &&
                it->Second().VoteEpoch == epoch)
            {
                ++count;
            }
        }
        return count;
    }

    bool IsFeasibleCandidate(
        TMasterId candidateId,
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
        for (TStatusTable::const_iterator it = StatusTable.begin();
            it != StatusTable.end();
            ++it)
        {
            const TStatus& currentCandidate = it->Second();
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
        LOG_DEBUG("Voting round completed (Round: %p)",
            this);

        ChooseVote();
    }
};

////////////////////////////////////////////////////////////////////////////////

RPC_SERVICE_METHOD_IMPL(TElectionManager, PingFollower)
{
    UNUSED(response);

    TMasterEpoch epoch = GuidFromProtoGuid(request->GetEpoch());
    TMasterId leaderId = request->GetLeaderId();

    context->SetRequestInfo("Epoch: %s, LeaderId: %d",
        ~epoch.ToString(),
        leaderId);

    if (State != TProxy::EState::Following)
        ythrow TServiceException(EErrorCode::InvalidState) <<
               Sprintf("Ping from a leader while in an invalid state (LeaderId: %d, Epoch: %s, State: %s)",
                   leaderId,
                   ~epoch.ToString(),
                   ~State.ToString());

    if (leaderId != LeaderId)
        ythrow TServiceException(EErrorCode::InvalidLeader) <<
               Sprintf("Ping from an invalid leader: expected %d, got %d",
                   LeaderId,
                   leaderId);

    if (epoch != Epoch)
        ythrow TServiceException(EErrorCode::InvalidEpoch) <<
               Sprintf("Ping with invalid epoch from leader %d: expected %s, got %s",
                   leaderId,
                   ~Epoch.ToString(),
                   ~epoch.ToString());

    TDelayedInvoker::Get()->Cancel(PingTimeoutCookie);

    PingTimeoutCookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TElectionManager::OnLeaderPingTimeout, this)
        ->Via(~EpochInvoker)
        ->Via(Invoker),
        TConfig::FollowerPingTimeout);

    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TElectionManager, GetStatus)
{
    UNUSED(request);

    context->SetRequestInfo("");

    TMasterPriority priority = ElectionCallbacks->GetPriority();

    response->SetState(State);
    response->SetVoteId(VoteId);
    response->SetPriority(priority);
    response->SetVoteEpoch(ProtoGuidFromGuid(VoteEpoch));

    context->SetResponseInfo("State: %s, VoteId: %d, Priority: %s, VoteEpoch: %s",
        ~State.ToString(),
        VoteId,
        ~ElectionCallbacks->FormatPriority(priority),
        ~VoteEpoch.ToString());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

void TElectionManager::Reset()
{
    State = TProxy::EState::Stopped;
    VoteId = InvalidMasterId;
    LeaderId = InvalidMasterId;
    VoteEpoch = TGuid();
    Epoch = TGuid();
    EpochStart = TInstant();
    if (~EpochInvoker != NULL) {
        EpochInvoker->Cancel();
        EpochInvoker.Drop();
    }
    AliveFollowers.clear();
    PotentialFollowers.clear();
    PingTimeoutCookie.Drop();
}

void TElectionManager::OnLeaderPingTimeout()
{
    YASSERT(State == TProxy::EState::Following);
    
    LOG_INFO("No recurrent ping from leader within timeout");
    
    StopFollowing();
    
    StartVoteForSelf();
}

void TElectionManager::DoStart()
{
    YASSERT(State == TProxy::EState::Stopped);

    StartVoteForSelf();
}

void TElectionManager::DoStop()
{
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
            YASSERT(false);
            break;
    }
}

void TElectionManager::StartVoteFor(TMasterId voteId, const TMasterEpoch& voteEpoch)
{
    State = TProxy::EState::Voting;
    VoteId = voteId;
    VoteEpoch = voteEpoch;
    StartVotingRound();
}

void TElectionManager::StartVoteForSelf()
{
    State = TProxy::EState::Voting;
    VoteId = CellManager->GetSelfId();
    VoteEpoch = TGuid::Create();

    YASSERT(~EpochInvoker == NULL);
    EpochInvoker = new TCancelableInvoker();

    TMasterPriority priority = ElectionCallbacks->GetPriority();

    LOG_DEBUG("Voting for self (Priority: %s, VoteEpoch: %s)",
                ~ElectionCallbacks->FormatPriority(priority),
                ~VoteEpoch.ToString());

    StartVotingRound();
}

void TElectionManager::StartVotingRound()
{
    YASSERT(State == TProxy::EState::Voting);
    TVotingRound::TPtr round = new TVotingRound(this);
    round->Run();
}

void TElectionManager::StartFollowing(
    TMasterId leaderId,
    const TMasterEpoch& epoch)
{
    State = TProxy::EState::Following;
    VoteId = leaderId;
    VoteEpoch = epoch;

    StartEpoch(leaderId, epoch);

    PingTimeoutCookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TElectionManager::OnLeaderPingTimeout, this)
        ->Via(~EpochInvoker)
        ->Via(Invoker),
        TConfig::ReadyToFollowTimeout);

    LOG_INFO("Starting following (LeaderId: %d, Epoch: %s)",
        LeaderId,
        ~Epoch.ToString());

    ElectionCallbacks->StartFollowing(LeaderId, Epoch);
}

void TElectionManager::StartLeading()
{
    State = TProxy::EState::Leading;
    YASSERT(VoteId == CellManager->GetSelfId());

    // Initialize followers state.
    for (TMasterId i = 0; i < CellManager->GetMasterCount(); ++i) {
        AliveFollowers.insert(i);
        PotentialFollowers.insert(i);
    }
    
    StartEpoch(CellManager->GetSelfId(), VoteEpoch);

    // Send initial pings.
    YASSERT(~FollowerPinger == NULL);
    FollowerPinger = new TFollowerPinger(this);
    FollowerPinger->Run();

    LOG_INFO("Starting leading (Epoch: %s)", ~Epoch.ToString());
    ElectionCallbacks->StartLeading(Epoch);
}

void TElectionManager::StopLeading()
{
    YASSERT(State == TProxy::EState::Leading);
    
    LOG_INFO("Stopping leading (Epoch: %s)",
               ~Epoch.ToString());

    YASSERT(~FollowerPinger != NULL);
    FollowerPinger->Cancel();
    FollowerPinger.Drop();

    StopEpoch();
    
    Reset();

    ElectionCallbacks->StopLeading();
}

void TElectionManager::StopFollowing()
{
    YASSERT(State == TProxy::EState::Following);

    LOG_INFO("Stopping following (LeaderId: %d, Epoch: %s)",
        LeaderId,
        ~Epoch.ToString());
    
    StopEpoch();
    
    Reset();
    
    ElectionCallbacks->StopFollowing();
}

IInvoker::TPtr TElectionManager::GetEpochInvoker() const
{
    return ~EpochInvoker;
}

void TElectionManager::StartEpoch(TMasterId leaderId, const TMasterEpoch& epoch)
{
    LeaderId = leaderId;
    Epoch = epoch;
    EpochStart = Now();
}

void TElectionManager::StopEpoch()
{
    LeaderId = InvalidMasterId;
    Epoch = TGuid();
    EpochStart = TInstant();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
