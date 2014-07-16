#include "stdafx.h"
#include "election_manager.h"
#include "config.h"
#include "private.h"

#include <core/concurrency/delayed_executor.h>
#include <core/concurrency/thread_affinity.h>

#include <core/concurrency/parallel_awaiter.h>

#include <core/ytree/fluent.h>

#include <core/rpc/service_detail.h>
#include <core/rpc/server.h>

#include <core/logging/log.h>

#include <ytlib/election/cell_manager.h>
#include <ytlib/election/election_service_proxy.h>

namespace NYT {
namespace NElection {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

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

    void Initialize();
    void Finalize();

    void Start();
    void Stop();

    TYsonProducer GetMonitoringProducer();

    TEpochContextPtr GetEpochContext();

private:
    class TVotingRound;
    typedef TIntrusivePtr<TVotingRound> TVotingRoundPtr;

    class TFollowerPinger;
    typedef TIntrusivePtr<TFollowerPinger> TFollowerPingerPtr;


    TElectionManagerConfigPtr Config;
    TCellManagerPtr CellManager;
    IInvokerPtr ControlInvoker;
    IElectionCallbacksPtr ElectionCallbacks;
    NRpc::IServerPtr RpcServer;

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

    NConcurrency::TDelayedExecutor::TCookie PingTimeoutCookie;
    TFollowerPingerPtr FollowerPinger;

    NLog::TLogger Logger;


    // Corresponds to #ControlInvoker.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);

    void Reset();
    void OnFollowerPingTimeout();

    void DoFinalize();
    void DoStart();
    void DoStop();

    bool CheckQuorum();

    void StartVotingRound();
    void StartVoteFor(TPeerId voteId, const TEpochId& voteEpochId);
    void StartVoteForSelf();

    void StartLeading();
    void StartFollowing(TPeerId leaderId, const TEpochId& epoch);
    void StopLeading();
    void StopFollowing();

    void InitEpochContext(TPeerId leaderId, const TEpochId& epoch);
    void SetState(EPeerState newState);

    void OnPeerReconfigured(TPeerId peerId);

};

////////////////////////////////////////////////////////////////////////////////

class TElectionManager::TImpl::TFollowerPinger
    : public TRefCounted
{
public:
    explicit TFollowerPinger(TImplPtr owner)
        : Owner(owner)
        , Awaiter(New<TParallelAwaiter>(Owner->ControlEpochInvoker))
        , Logger(Owner->Logger)
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

    NLog::TLogger& Logger;


    void SendPing(TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (Awaiter->IsCanceled())
            return;

        auto channel = Owner->CellManager->GetPeerChannel(peerId);
        if (!channel) {
            SchedulePing(peerId);
            return;
        }

        LOG_DEBUG("Sending ping to follower %d", peerId);

        TElectionServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Owner->Config->RpcTimeout);

        auto request = proxy.PingFollower();
        request->set_leader_id(Owner->CellManager->GetSelfId());
        ToProto(request->mutable_epoch_id(), Owner->EpochContext->EpochId);

        Awaiter->Await(
            request->Invoke(),
            BIND(&TFollowerPinger::OnPingResponse, MakeStrong(this), peerId)
                .Via(Owner->ControlEpochInvoker));
    }

    void SchedulePing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        TDelayedExecutor::Submit(
            BIND(&TFollowerPinger::SendPing, MakeWeak(this), id)
                .Via(Owner->ControlEpochInvoker),
            Owner->Config->FollowerPingInterval);
    }

    void OnPingResponse(TPeerId id, TElectionServiceProxy::TRspPingFollowerPtr response)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);
        YCHECK(Owner->State == EPeerState::Leading);

        if (response->IsOK()) {
            OnPingResponseSuccess(id, response);
        } else {
            OnPingResponseFailure(id, response);
        }
    }

    void OnPingResponseSuccess(TPeerId id, TElectionServiceProxy::TRspPingFollowerPtr response)
    {
        LOG_DEBUG("Ping reply from follower %d", id);

        if (Owner->PotentialFollowers.find(id) != Owner->PotentialFollowers.end()) {
            LOG_INFO("Follower %d is up, first success", id);
            Owner->PotentialFollowers.erase(id);
        } else if (Owner->AliveFollowers.find(id) == Owner->AliveFollowers.end()) {
            LOG_INFO("Follower %d is up", id);
            Owner->AliveFollowers.insert(id);
        }

        SchedulePing(id);
    }

    void OnPingResponseFailure(TPeerId id, TElectionServiceProxy::TRspPingFollowerPtr response)
    {
        auto error = response->GetError();
        auto code = error.GetCode();

        if (code == NElection::EErrorCode::InvalidState ||
            code == NElection::EErrorCode::InvalidLeader ||
            code == NElection::EErrorCode::InvalidEpoch)
        {
            // These errors are possible during grace period.
            if (Owner->PotentialFollowers.find(id) == Owner->PotentialFollowers.end()) {
                if (Owner->AliveFollowers.erase(id) > 0) {
                    LOG_WARNING(error, "Error pinging follower %d, considered down",
                        id);
                }
            } else {
                if (TInstant::Now() > Owner->EpochContext->StartTime + Owner->Config->FollowerGracePeriod) {
                    LOG_WARNING(error, "Error pinging follower %d, no success within grace period, considered down",
                        id);
                    Owner->PotentialFollowers.erase(id);
                    Owner->AliveFollowers.erase(id);
                } else {
                    LOG_INFO(error, "Error pinging follower %d, will retry later",
                        id);
                }
            }
        } else {
            if (Owner->AliveFollowers.erase(id) > 0) {
                LOG_WARNING(error, "Error pinging follower %d, considered down",
                    id);
                Owner->PotentialFollowers.erase(id);
            }
        }

        if (!Owner->CheckQuorum())
            return;

        if (code == NRpc::EErrorCode::Timeout) {
            SendPing(id);
        } else {
            SchedulePing(id);
        }
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
        , Logger(Owner->Logger)
    {
        Logger.AddTag("RoundId: %v, VoteEpochId: %v",
            TGuid::Create(),
            Owner->VoteEpochId);
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);
        YCHECK(Owner->State == EPeerState::Voting);

        auto callbacks = Owner->ElectionCallbacks;
        auto cellManager = Owner->CellManager;
        auto priority = callbacks->GetPriority();

        LOG_DEBUG("New voting round started (VoteId: %d, Priority: %s)",
            Owner->VoteId,
            ~callbacks->FormatPriority(priority));

        ProcessVote(
            cellManager->GetSelfId(),
            TStatus(
                Owner->State,
                Owner->VoteId,
                priority,
                Owner->VoteEpochId));

        for (TPeerId id = 0; id < cellManager->GetPeerCount(); ++id) {
            if (id == cellManager->GetSelfId())
                continue;

            auto channel = Owner->CellManager->GetPeerChannel(id);
            if (!channel)
                continue;

            TElectionServiceProxy proxy(channel);
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

    NLog::TLogger Logger;


    void ProcessVote(TPeerId id, const TStatus& status)
    {
        YCHECK(id != InvalidPeerId);
        StatusTable[id] = status;

        for (const auto& pair : StatusTable) {
            if (CheckForLeader(pair.first, pair.second)) {
                break;
            }
        }
    }

    void OnResponse(TPeerId id, TElectionServiceProxy::TRspGetStatusPtr response)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (!response->IsOK()) {
            LOG_INFO(response->GetError(), "Error requesting status from peer %d",
                id);
            return;
        }

        auto state = EPeerState(response->state());
        auto vote = response->vote_id();
        auto priority = response->priority();
        auto epochId = FromProto<TEpochId>(response->vote_epoch_id());

        LOG_DEBUG("Received status from peer %d (State: %s, VoteId: %d, Priority: %s)",
            id,
            ~ToString(state),
            vote,
            ~Owner->ElectionCallbacks->FormatPriority(priority));

        ProcessVote(id, TStatus(state, vote, priority, epochId));
    }

    bool CheckForLeader(TPeerId candidateId, const TStatus& candidateStatus)
    {
        if (!IsFeasibleLeader(candidateId, candidateStatus)) {
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
        int quorum = Owner->CellManager->GetQuorumCount();

        // Check for quorum.
        if (voteCount < quorum) {
            return false;
        }

        LOG_DEBUG("Candidate %d has quorum: %d >= %d",
            candidateId,
            voteCount,
            quorum);

        Awaiter->Cancel();

        // Become a leader or a follower.
        if (candidateId == Owner->CellManager->GetSelfId()) {
            ControlEpochInvoker->Invoke(BIND(
                &TImpl::StartLeading,
                Owner));
        } else {
            ControlEpochInvoker->Invoke(BIND(
                &TImpl::StartFollowing,
                Owner,
                candidateId,
                candidateStatus.VoteEpochId));
        }

        return true;
    }

    int CountVotesFor(TPeerId candidateId, const TEpochId& epochId) const
    {
        int result = 0;
        for (const auto& pair : StatusTable) {
            if (pair.second.VoteId == candidateId && pair.second.VoteEpochId == epochId) {
                ++result;
            }
        }
        return result;
    }

    bool IsFeasibleLeader(TPeerId candidateId, const TStatus& candidateStatus) const
    {
        // He must be voting for himself.
        if (candidateId != candidateStatus.VoteId) {
            return false;
        }

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
    static bool IsBetterCandidate(const TStatus& lhs, const TStatus& rhs)
    {
        if (lhs.Priority > rhs.Priority)
            return true;

        if (lhs.Priority < rhs.Priority)
            return false;

        return lhs.VoteId < rhs.VoteId;
    }

    void OnComplete()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        LOG_DEBUG("Voting round completed");

        // Choose the best vote.
        TNullable<TStatus> bestCandidate;
        for (const auto& pair : StatusTable) {
            const auto& currentCandidate = pair.second;
            if (StatusTable.find(currentCandidate.VoteId) != StatusTable.end() &&
                (!bestCandidate || IsBetterCandidate(currentCandidate, *bestCandidate)))
            {
                bestCandidate = currentCandidate;
            }
        }

        if (bestCandidate) {
            // Extract the status of the best candidate.
            // His status must be present in the table by the above checks.
            const auto& candidateStatus = StatusTable[bestCandidate->VoteId];
            Owner->StartVoteFor(candidateStatus.VoteId, candidateStatus.VoteEpochId);
        } else {
            Owner->StartVoteForSelf();
        }
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
        NRpc::TServiceId(TElectionServiceProxy::GetServiceName(), cellManager->GetCellGuid()),
        ElectionLogger.GetCategory())
    , Config(config)
    , CellManager(cellManager)
    , ControlInvoker(controlInvoker)
    , ElectionCallbacks(electionCallbacks)
    , RpcServer(rpcServer)
    , State(EPeerState::Stopped)
    , VoteId(InvalidPeerId)
    , Logger(ElectionLogger)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(ControlInvoker);
    YCHECK(ElectionCallbacks);
    YCHECK(RpcServer);
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);

    Logger.AddTag("CellGuid: %v, SelfId: %v",
        CellManager->GetCellGuid(),
        CellManager->GetSelfId());

    Reset();

    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));

    CellManager->SubscribePeerReconfigured(
        BIND(&TImpl::OnPeerReconfigured, Unretained(this))
            .Via(ControlInvoker));
}

void TElectionManager::TImpl::Initialize()
{
    VERIFY_THREAD_AFFINITY_ANY();

    RpcServer->RegisterService(this);
}

void TElectionManager::TImpl::Finalize()
{
    VERIFY_THREAD_AFFINITY_ANY();

    ControlInvoker->Invoke(BIND(&TImpl::DoFinalize, MakeStrong(this)));

    RpcServer->UnregisterService(this);
}

void TElectionManager::TImpl::Start()
{
    ControlInvoker->Invoke(BIND(&TImpl::DoStart, MakeStrong(this)));
}

void TElectionManager::TImpl::Stop()
{
    ControlInvoker->Invoke(BIND(&TImpl::DoStop, MakeStrong(this)));
}

TYsonProducer TElectionManager::TImpl::GetMonitoringProducer()
{
    auto this_ = MakeStrong(this);
    return BIND([this, this_] (IYsonConsumer* consumer) {
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
    });
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

void TElectionManager::TImpl::OnFollowerPingTimeout()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Following);

    LOG_INFO("No recurrent ping from leader within timeout");

    StopFollowing();
}

void TElectionManager::TImpl::DoFinalize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    DoStop();
}

void TElectionManager::TImpl::DoStart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State) {
        case EPeerState::Stopped:
            StartVoteForSelf();
            break;

        case EPeerState::Voting:
            break;

        case EPeerState::Leading:
            LOG_INFO("Leader restart forced");
            StopLeading();
            StartVoteForSelf();
            break;

        case EPeerState::Following:
            LOG_INFO("Follower restart forced");
            StopFollowing();
            StartVoteForSelf();
            break;

        default:
            YUNREACHABLE();
    }
}

void TElectionManager::TImpl::DoStop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State) {
        case EPeerState::Stopped:
        case EPeerState::Voting:
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

    Reset();
}

bool TElectionManager::TImpl::CheckQuorum()
{
    if (static_cast<int>(AliveFollowers.size()) >= CellManager->GetQuorumCount()) {
        return true;
    }

    LOG_WARNING("Quorum is lost");
    
    StopLeading();

    return false;
}

void TElectionManager::TImpl::StartVoteFor(TPeerId voteId, const TEpochId& voteEpoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Voting);
    VoteId = voteId;
    VoteEpochId = voteEpoch;

    LOG_DEBUG("Voting for another candidate (VoteId: %d, VoteEpochId: %s)",
        VoteId,
        ~ToString(VoteEpochId));

    StartVotingRound();
}

void TElectionManager::TImpl::StartVoteForSelf()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Voting);
    VoteId = CellManager->GetSelfId();
    VoteEpochId = TGuid::Create();

    if (EpochContext) {
        EpochContext->CancelableContext->Cancel();
        EpochContext.Reset();
    }

    EpochContext = New<TEpochContext>();
    ControlEpochInvoker = EpochContext->CancelableContext->CreateInvoker(ControlInvoker);

    LOG_DEBUG("Voting for self (VoteId: %d, Priority: %s, VoteEpochId: %s)",
        VoteId,
        ~ElectionCallbacks->FormatPriority(ElectionCallbacks->GetPriority()),
        ~ToString(VoteEpochId));

    StartVotingRound();
}

void TElectionManager::TImpl::StartVotingRound()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Voting);

    auto round = New<TVotingRound>(this);
    TDelayedExecutor::Submit(
        BIND(&TVotingRound::Run, round)
            .Via(ControlEpochInvoker),
        Config->VotingRoundInterval);
}

void TElectionManager::TImpl::StartFollowing(
    TPeerId leaderId,
    const TEpochId& epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Following);
    VoteId = leaderId;
    VoteEpochId = epochId;

    InitEpochContext(leaderId, epochId);

    PingTimeoutCookie = TDelayedExecutor::Submit(
        BIND(&TImpl::OnFollowerPingTimeout, MakeWeak(this))
            .Via(ControlEpochInvoker),
        Config->ReadyToFollowTimeout);

    LOG_INFO("Started following (LeaderId: %d, EpochId: %s)",
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

    LOG_INFO("Started leading (EpochId: %s)",
        ~ToString(EpochContext->EpochId));

    ElectionCallbacks->OnStartLeading();
}

void TElectionManager::TImpl::StopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Leading);

    LOG_INFO("Stopped leading (EpochId: %s)",
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

    LOG_INFO("Stopped following (LeaderId: %d, EpochId: %s)",
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
    LOG_INFO("State changed: %s -> %s",
        ~ToString(State),
        ~ToString(newState));
    State = newState;
}

void TElectionManager::TImpl::OnPeerReconfigured(TPeerId peerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (peerId == CellManager->GetSelfId()) {
        if (State == EPeerState::Leading || State == EPeerState::Following) {
            DoStart();
        }
    } else {
        if (State == EPeerState::Leading) {
            PotentialFollowers.erase(peerId);
            AliveFollowers.erase(peerId);
            CheckQuorum();
        } else if (State == EPeerState::Following && peerId == EpochContext->LeaderId) {
            DoStart();
        }
    }
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
            NElection::EErrorCode::InvalidState,
            "Received ping in invalid state: expected %s, actual %s",
            ~FormatEnum(EPeerState(EPeerState::Following)).Quote(),
            ~FormatEnum(State).Quote());
    }

    if (epochId != EpochContext->EpochId) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidEpoch,
            "Received ping with invalid epoch: expected %s, received %s",
            ~ToString(EpochContext->EpochId),
            ~ToString(epochId));
    }

    if (leaderId != EpochContext->LeaderId) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidLeader,
            "Ping from an invalid leader: expected %d, received %d",
            EpochContext->LeaderId,
            leaderId);
    }

    TDelayedExecutor::Cancel(PingTimeoutCookie);

    PingTimeoutCookie = TDelayedExecutor::Submit(
        BIND(&TImpl::OnFollowerPingTimeout, MakeWeak(this))
            .Via(ControlEpochInvoker),
        Config->FollowerPingTimeout);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TElectionManager::TImpl, GetStatus)
{
    UNUSED(request);
    VERIFY_THREAD_AFFINITY(ControlThread);

    context->SetRequestInfo();

    auto priority = ElectionCallbacks->GetPriority();

    response->set_state(State);
    response->set_vote_id(VoteId);
    response->set_priority(priority);
    ToProto(response->mutable_vote_epoch_id(), VoteEpochId);
    response->set_self_id(CellManager->GetSelfId());

    context->SetResponseInfo("State: %s, VoteId: %d, Priority: %s, VoteEpochId: %s",
        ~ToString(State),
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
{
    Impl->Initialize();
}

TElectionManager::~TElectionManager()
{
    Impl->Finalize();
}

void TElectionManager::Start()
{
    Impl->Start();
}

void TElectionManager::Stop()
{
    Impl->Stop();
}

TYsonProducer TElectionManager::GetMonitoringProducer()
{
    return Impl->GetMonitoringProducer();
}

TEpochContextPtr TElectionManager::GetEpochContext()
{
    return Impl->GetEpochContext();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
