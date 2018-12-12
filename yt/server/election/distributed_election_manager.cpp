#include "election_manager.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/election/cell_manager.h>
#include <yt/ytlib/election/election_service_proxy.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/lease_manager.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/fluent.h>

namespace NYT::NElection {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDistributedElectionManager)

class TDistributedElectionManager
    : public TServiceBase
    , public IElectionManager
{
public:
    TDistributedElectionManager(
        TDistributedElectionManagerConfigPtr config,
        TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker,
        IElectionCallbacksPtr electionCallbacks,
        IServerPtr rpcServer);

    virtual void Initialize() override;
    virtual void Finalize() override;

    virtual void Participate() override;
    virtual void Abandon() override;

    virtual TYsonProducer GetMonitoringProducer() override;

private:
    class TVotingRound;

    class TFollowerPinger;
    typedef TIntrusivePtr<TFollowerPinger> TFollowerPingerPtr;

    const TDistributedElectionManagerConfigPtr Config;
    const TCellManagerPtr CellManager;
    const IInvokerPtr ControlInvoker;
    const IElectionCallbacksPtr ElectionCallbacks;
    const IServerPtr RpcServer_;

    EPeerState State = EPeerState::Stopped;

    // Voting parameters.
    TPeerId VoteId = InvalidPeerId;
    TEpochId VoteEpochId;

    // Epoch parameters.
    TEpochContextPtr EpochContext;
    IInvokerPtr ControlEpochInvoker;

    THashSet<TPeerId> AliveFollowers;
    THashSet<TPeerId> PotentialFollowers;

    TLease LeaderPingLease;
    TFollowerPingerPtr FollowerPinger;


    // Corresponds to #ControlInvoker.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);

    void Reset();

    void OnLeaderPingLeaseExpired();

    void DoParticipate();
    void DoAdandon();

    bool CheckQuorum();

    bool IsVotingPeer();

    void StartVotingRound();
    void ContinueVoting(TPeerId voteId, const TEpochId& voteEpochId);
    void StartVoting();

    void StartLeading();
    void StartFollowing(TPeerId leaderId, const TEpochId& epoch);

    void StopLeading();
    void StopFollowing();

    void InitEpochContext(TPeerId leaderId, const TEpochId& epoch);
    void SetState(EPeerState newState);

    void OnPeerReconfigured(TPeerId peerId);

};

DEFINE_REFCOUNTED_TYPE(TDistributedElectionManager)

////////////////////////////////////////////////////////////////////////////////

// Also pings observers.
class TDistributedElectionManager::TFollowerPinger
    : public TRefCounted
{
public:
    explicit TFollowerPinger(TDistributedElectionManagerPtr owner)
        : Owner(owner)
        , Logger(Owner->Logger)
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        const auto& cellManager = Owner->CellManager;
        for (TPeerId id = 0; id < cellManager->GetTotalPeerCount(); ++id) {
            if (id == cellManager->GetSelfPeerId())
                continue;

            SendPing(id);
        }
    }

private:
    const TDistributedElectionManagerPtr Owner;
    const NLogging::TLogger Logger;


    void SendPing(TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        auto channel = Owner->CellManager->GetPeerChannel(peerId);
        if (!channel) {
            SchedulePing(peerId);
            return;
        }

        YT_LOG_DEBUG("Sending ping to follower (PeerId: %v)", peerId);

        TElectionServiceProxy proxy(channel);
        auto req = proxy.PingFollower();
        req->SetTimeout(Owner->Config->FollowerPingRpcTimeout);
        req->set_leader_id(Owner->CellManager->GetSelfPeerId());
        ToProto(req->mutable_epoch_id(), Owner->EpochContext->EpochId);

        req->Invoke().Subscribe(
            BIND(&TFollowerPinger::OnPingResponse, MakeStrong(this), peerId)
                .Via(Owner->ControlEpochInvoker));
    }

    void SchedulePing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        TDelayedExecutor::Submit(
            BIND(&TFollowerPinger::SendPing, MakeStrong(this), id)
                .Via(Owner->ControlEpochInvoker),
            Owner->Config->FollowerPingPeriod);
    }

    void OnPingResponse(TPeerId id, const TElectionServiceProxy::TErrorOrRspPingFollowerPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);
        YCHECK(Owner->State == EPeerState::Leading);

        if (rspOrError.IsOK()) {
            OnPingResponseSuccess(id, rspOrError.Value());
        } else {
            OnPingResponseFailure(id, rspOrError);
        }
    }

    void OnPingResponseSuccess(TPeerId id, TElectionServiceProxy::TRspPingFollowerPtr rsp)
    {
        YT_LOG_DEBUG("Ping reply from follower (PeerId: %v)", id);

        if (Owner->PotentialFollowers.find(id) != Owner->PotentialFollowers.end()) {
            YT_LOG_INFO("Follower is up, first success (PeerId: %v)", id);
            YCHECK(Owner->PotentialFollowers.erase(id) == 1);
        } else if (Owner->AliveFollowers.find(id) == Owner->AliveFollowers.end()) {
            YT_LOG_INFO("Follower is up (PeerId: %v)", id);
            YCHECK(Owner->AliveFollowers.insert(id).second);
        }

        SchedulePing(id);
    }

    void OnPingResponseFailure(TPeerId id, const TError& error)
    {
        auto code = error.GetCode();
        if (code == NElection::EErrorCode::InvalidState ||
            code == NElection::EErrorCode::InvalidLeader ||
            code == NElection::EErrorCode::InvalidEpoch)
        {
            // These errors are possible during grace period.
            if (Owner->PotentialFollowers.find(id) == Owner->PotentialFollowers.end()) {
                if (Owner->AliveFollowers.erase(id) > 0) {
                    YT_LOG_WARNING(error, "Error pinging follower %v, considered down",
                        id);
                }
            } else {
                if (TInstant::Now() > Owner->EpochContext->StartTime + Owner->Config->FollowerGraceTimeout) {
                    YT_LOG_WARNING(error, "Error pinging follower %v, no success within grace period, considered down",
                        id);
                    Owner->PotentialFollowers.erase(id);
                    Owner->AliveFollowers.erase(id);
                } else {
                    YT_LOG_INFO(error, "Error pinging follower %v, will retry later",
                        id);
                }
            }
        } else {
            if (Owner->AliveFollowers.erase(id) > 0) {
                YT_LOG_WARNING(error, "Error pinging follower %v, considered down",
                    id);
                Owner->PotentialFollowers.erase(id);
            }
        }

        if (!Owner->CheckQuorum())
            return;

        if (code == NYT::EErrorCode::Timeout) {
            SendPing(id);
        } else {
            SchedulePing(id);
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

class TDistributedElectionManager::TVotingRound
    : public TRefCounted
{
public:
    explicit TVotingRound(TDistributedElectionManagerPtr owner)
        : Owner(owner)
    {
        Logger = Owner->Logger;
        Logger.AddTag("RoundId: %v, VoteEpochId: %v",
            TGuid::Create(),
            Owner->VoteEpochId);
    }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);
        YCHECK(Owner->State == EPeerState::Voting);

        auto cellManager = Owner->CellManager;

        YT_LOG_DEBUG("New voting round started");

        if (Owner->IsVotingPeer()) {
            ProcessVote(
                cellManager->GetSelfPeerId(),
                TStatus(
                    Owner->State,
                    Owner->VoteId,
                    Owner->ElectionCallbacks->GetPriority(),
                    Owner->VoteEpochId));
        }

        std::vector<TFuture<void>> asyncResults;
        for (TPeerId id = 0; id < cellManager->GetTotalPeerCount(); ++id) {
            if (id == cellManager->GetSelfPeerId())
                continue;

            auto channel = Owner->CellManager->GetPeerChannel(id);
            if (!channel)
                continue;

            const auto& peerConfig = cellManager->GetPeerConfig(id);
            if (!peerConfig.Voting)
                continue;

            TElectionServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner->Config->ControlRpcTimeout);

            auto req = proxy.GetStatus();
            asyncResults.push_back(
                req->Invoke().Apply(
                    BIND(&TVotingRound::OnResponse, MakeStrong(this), id)
                        .AsyncVia(Owner->ControlEpochInvoker)));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TVotingRound::OnComplete, MakeStrong(this))
                .Via(Owner->ControlEpochInvoker));
    }

private:
    const TDistributedElectionManagerPtr Owner;

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

    typedef THashMap<TPeerId, TStatus> TStatusTable;

    TStatusTable StatusTable;

    bool Finished = false;

    NLogging::TLogger Logger;


    void ProcessVote(TPeerId id, const TStatus& status)
    {
        YT_LOG_DEBUG("Vote received (PeerId: %v, State: %v, VoteId: %v, Priority: %v)",
            id,
            status.State,
            status.VoteId,
            Owner->ElectionCallbacks->FormatPriority(status.Priority));

        YCHECK(id != InvalidPeerId);
        StatusTable[id] = status;

        for (const auto& pair : StatusTable) {
            if (CheckForLeader(pair.first, pair.second)) {
                break;
            }
        }
    }

    void OnResponse(TPeerId id, const TElectionServiceProxy::TErrorOrRspGetStatusPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (Finished)
            return;

        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error requesting status from peer %v",
                id);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto state = EPeerState(rsp->state());
        auto voteId = rsp->vote_id();
        auto priority = rsp->priority();
        auto epochId = FromProto<TEpochId>(rsp->vote_epoch_id());
        ProcessVote(id, TStatus(state, voteId, priority, epochId));
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
            candidateId == Owner->CellManager->GetSelfPeerId()
            ? Owner->VoteEpochId
            : candidateStatus.VoteEpochId;

        // Count votes (including self) and quorum.
        int voteCount = CountVotesFor(candidateId, candidateEpochId);
        int quorumCount = Owner->CellManager->GetQuorumPeerCount();

        // Check for quorum.
        if (voteCount < quorumCount) {
            return false;
        }

        YT_LOG_DEBUG("Candidate has quorum (PeerId: %v, VoteCount: %v, QuorumCount: %v)",
            candidateId,
            voteCount,
            quorumCount);

        Finished = true;

        // Become a leader or a follower.
        if (candidateId == Owner->CellManager->GetSelfPeerId()) {
            Owner->ControlEpochInvoker->Invoke(BIND(
                &TDistributedElectionManager::StartLeading,
                Owner));
        } else {
            Owner->ControlEpochInvoker->Invoke(BIND(
                &TDistributedElectionManager::StartFollowing,
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

        if (candidateId == Owner->CellManager->GetSelfPeerId()) {
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
        if (lhs.Priority > rhs.Priority) {
            return true;
        }

        if (lhs.Priority < rhs.Priority) {
            return false;
        }

        return lhs.VoteId < rhs.VoteId;
    }

    void OnComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY(Owner->ControlThread);

        if (Finished)
            return;

        YT_LOG_DEBUG("Voting round completed");

        // Choose the best vote.
        std::optional<TStatus> bestCandidate;
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
            Owner->ContinueVoting(candidateStatus.VoteId, candidateStatus.VoteEpochId);
        } else {
            Owner->StartVoting();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TDistributedElectionManager::TDistributedElectionManager(
    TDistributedElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    IServerPtr rpcServer)
    : TServiceBase(
        controlInvoker,
        TElectionServiceProxy::GetDescriptor(),
        NLogging::TLogger(ElectionLogger)
            .AddTag("CellId: %v, SelfPeerId: %v",
                cellManager->GetCellId(),
                cellManager->GetSelfPeerId()),
        cellManager->GetCellId())
    , Config(config)
    , CellManager(cellManager)
    , ControlInvoker(controlInvoker)
    , ElectionCallbacks(electionCallbacks)
    , RpcServer_(rpcServer)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(ControlInvoker);
    YCHECK(ElectionCallbacks);
    VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker, ControlThread);

    Reset();

    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));

    CellManager->SubscribePeerReconfigured(
        BIND(&TDistributedElectionManager::OnPeerReconfigured, MakeWeak(this))
            .Via(ControlInvoker));
}

void TDistributedElectionManager::Initialize()
{
    RpcServer_->RegisterService(this);
}

void TDistributedElectionManager::Finalize()
{
    Abandon();
    RpcServer_->UnregisterService(this);
}

void TDistributedElectionManager::Participate()
{
    ControlInvoker->Invoke(BIND(&TDistributedElectionManager::DoParticipate, MakeWeak(this)));
}

void TDistributedElectionManager::Abandon()
{
    ControlInvoker->Invoke(BIND(&TDistributedElectionManager::DoAdandon, MakeWeak(this)));
}

TYsonProducer TDistributedElectionManager::GetMonitoringProducer()
{
    return BIND([=, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
        auto epochContext = EpochContext;
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Value(State)
                .Item("peers").BeginList()
                    .DoFor(0, CellManager->GetTotalPeerCount(), [=] (TFluentList fluent, TPeerId id) {
                        fluent.Item().Value(CellManager->GetPeerConfig(id));
                    })
                .EndList()
                .DoIf(epochContext.operator bool(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("leader_id").Value(epochContext->LeaderId)
                        .Item("epoch_id").Value(epochContext->EpochId);
                })
                .Item("vote_id").Value(VoteId)
            .EndMap();
    });
}

void TDistributedElectionManager::Reset()
{
    // May be called from ControlThread and also from ctor.

    SetState(EPeerState::Stopped);

    VoteId = InvalidPeerId;

    if (EpochContext) {
        EpochContext->CancelableContext->Cancel();
    }
    EpochContext.Reset();

    AliveFollowers.clear();
    PotentialFollowers.clear();
    TLeaseManager::CloseLease(LeaderPingLease);
    LeaderPingLease.Reset();
}

void TDistributedElectionManager::OnLeaderPingLeaseExpired()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_INFO("No recurrent ping from leader within timeout");

    YCHECK(State == EPeerState::Following);
    StopFollowing();
}

void TDistributedElectionManager::DoParticipate()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State) {
        case EPeerState::Stopped:
            StartVoting();
            break;

        case EPeerState::Voting:
            break;

        case EPeerState::Leading:
            YT_LOG_INFO("Leader restart forced");
            StopLeading();
            StartVoting();
            break;

        case EPeerState::Following:
            YT_LOG_INFO("Follower restart forced");
            StopFollowing();
            StartVoting();
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TDistributedElectionManager::DoAdandon()
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
            Y_UNREACHABLE();
    }

    Reset();
}

bool TDistributedElectionManager::CheckQuorum()
{
    if (AliveFollowers.size() >= CellManager->GetQuorumPeerCount()) {
        return true;
    }

    YT_LOG_WARNING("Quorum is lost");
    
    StopLeading();

    return false;
}

bool TDistributedElectionManager::IsVotingPeer()
{
    const auto& config = CellManager->GetSelfConfig();
    return config.Voting;
}

void TDistributedElectionManager::ContinueVoting(TPeerId voteId, const TEpochId& voteEpoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Voting);
    VoteId = voteId;
    VoteEpochId = voteEpoch;

    YT_LOG_DEBUG("Voting for another candidate (VoteId: %v, VoteEpochId: %v)",
        VoteId,
        VoteEpochId);

    StartVotingRound();
}

void TDistributedElectionManager::StartVoting()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (EpochContext) {
        EpochContext->CancelableContext->Cancel();
        EpochContext.Reset();
    }

    EpochContext = New<TEpochContext>();
    ControlEpochInvoker = EpochContext->CancelableContext->CreateInvoker(ControlInvoker);

    SetState(EPeerState::Voting);
    VoteEpochId = TGuid::Create();

    if (IsVotingPeer()) {
        VoteId = CellManager->GetSelfPeerId();
        YT_LOG_DEBUG("Voting for self (VoteId: %v, VoteEpochId: %v)",
            VoteId,
            VoteEpochId);
    } else {
        VoteId = InvalidPeerId;
        YT_LOG_DEBUG("Voting for nobody (VoteEpochId: %v)",
            VoteEpochId);
    }

    StartVotingRound();
}

void TDistributedElectionManager::StartVotingRound()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Voting);

    auto round = New<TVotingRound>(this);
    TDelayedExecutor::Submit(
        BIND(&TVotingRound::Run, round)
            .Via(ControlEpochInvoker),
        Config->VotingRoundPeriod);
}

void TDistributedElectionManager::StartLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Leading);
    YCHECK(VoteId == CellManager->GetSelfPeerId());

    // Initialize followers state.
    for (TPeerId id = 0; id < CellManager->GetTotalPeerCount(); ++id) {
        const auto& peerConfig = CellManager->GetPeerConfig(id);
        if (peerConfig.Voting) {
            AliveFollowers.insert(id);
            PotentialFollowers.insert(id);
        }
    }

    InitEpochContext(CellManager->GetSelfPeerId(), VoteEpochId);

    // Send initial pings.
    YCHECK(!FollowerPinger);
    FollowerPinger = New<TFollowerPinger>(this);
    FollowerPinger->Run();

    YT_LOG_INFO("Started leading (EpochId: %v)",
        EpochContext->EpochId);

    ElectionCallbacks->OnStartLeading(EpochContext);
}

void TDistributedElectionManager::StartFollowing(
    TPeerId leaderId,
    const TEpochId& epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Following);
    VoteId = leaderId;
    VoteEpochId = epochId;

    InitEpochContext(leaderId, epochId);

    LeaderPingLease = TLeaseManager::CreateLease(
        Config->LeaderPingTimeout,
        BIND(&TDistributedElectionManager::OnLeaderPingLeaseExpired, MakeWeak(this))
            .Via(ControlEpochInvoker));

    YT_LOG_INFO("Started following (LeaderId: %v, EpochId: %v)",
        EpochContext->LeaderId,
        EpochContext->EpochId);

    ElectionCallbacks->OnStartFollowing(EpochContext);
}

void TDistributedElectionManager::StopLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Leading);

    YT_LOG_INFO("Stopped leading (EpochId: %v)",
        EpochContext->EpochId);

    ElectionCallbacks->OnStopLeading();

    YCHECK(FollowerPinger);
    FollowerPinger.Reset();

    Reset();
}

void TDistributedElectionManager::StopFollowing()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(State == EPeerState::Following);

    YT_LOG_INFO("Stopped following (LeaderId: %v, EpochId: %v)",
        EpochContext->LeaderId,
        EpochContext->EpochId);

    ElectionCallbacks->OnStopFollowing();

    Reset();
}

void TDistributedElectionManager::InitEpochContext(TPeerId leaderId, const TEpochId& epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EpochContext->LeaderId = leaderId;
    EpochContext->EpochId = epochId;
    EpochContext->StartTime = TInstant::Now();
}

void TDistributedElectionManager::SetState(EPeerState newState)
{
    if (newState == State)
        return;

    // This generic message logged to simplify tracking state changes.
    YT_LOG_INFO("State changed: %v -> %v",
        State,
        newState);
    State = newState;
}

void TDistributedElectionManager::OnPeerReconfigured(TPeerId peerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (peerId == CellManager->GetSelfPeerId()) {
        if (State == EPeerState::Leading || State == EPeerState::Following) {
            DoParticipate();
        }
    } else {
        const auto& peerConfig = CellManager->GetPeerConfig(peerId);
        if (State == EPeerState::Leading && peerConfig.Voting) {
            PotentialFollowers.erase(peerId);
            AliveFollowers.erase(peerId);
            CheckQuorum();
        } else if (State == EPeerState::Following && peerId == EpochContext->LeaderId) {
            DoParticipate();
        }
    }
}

DEFINE_RPC_SERVICE_METHOD(TDistributedElectionManager, PingFollower)
{
    Y_UNUSED(response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto epochId = FromProto<TEpochId>(request->epoch_id());
    auto leaderId = request->leader_id();

    context->SetRequestInfo("Epoch: %v, LeaderId: %v",
        epochId,
        leaderId);

    if (State != EPeerState::Following) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidState,
            "Received ping in invalid state: expected %Qlv, actual %Qlv",
            EPeerState::Following,
            State);
    }

    if (epochId != EpochContext->EpochId) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidEpoch,
            "Received ping with invalid epoch: expected %v, received %v",
            EpochContext->EpochId,
            epochId);
    }

    if (leaderId != EpochContext->LeaderId) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidLeader,
            "Ping from an invalid leader: expected %v, received %v",
            EpochContext->LeaderId,
            leaderId);
    }

    TLeaseManager::RenewLease(LeaderPingLease);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TDistributedElectionManager, GetStatus)
{
    Y_UNUSED(request);
    VERIFY_THREAD_AFFINITY(ControlThread);

    context->SetRequestInfo();

    if (!IsVotingPeer()) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Not a voting peer");
    }

    auto priority = ElectionCallbacks->GetPriority();

    response->set_state(static_cast<int>(State));
    response->set_vote_id(VoteId);
    response->set_priority(priority);
    ToProto(response->mutable_vote_epoch_id(), VoteEpochId);
    response->set_self_id(CellManager->GetSelfPeerId());

    context->SetResponseInfo("State: %v, VoteId: %v, Priority: %v, VoteEpochId: %v",
        State,
        VoteId,
        ElectionCallbacks->FormatPriority(priority),
        VoteEpochId);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IElectionManagerPtr CreateDistributedElectionManager(
    TDistributedElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    IServerPtr rpcServer)
{
    return New<TDistributedElectionManager>(
        config,
        cellManager,
        controlInvoker,
        electionCallbacks,
        rpcServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
