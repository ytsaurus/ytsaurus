#include "election_manager.h"
#include "private.h"
#include "config.h"

#include <yt/ytlib/election/cell_manager.h>
#include <yt/ytlib/election/config.h>
#include <yt/ytlib/election/election_service_proxy.h>

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/lease_manager.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/rpc/service_detail.h>

#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/atomic_object.h>

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
    virtual void Abandon(const TError& error) override;
    virtual void ReconfigureCell(TCellManagerPtr cellManager) override;

    virtual TYsonProducer GetMonitoringProducer() override;

private:
    class TVotingRound;

    class TFollowerPinger;
    using TFollowerPingerPtr = TIntrusivePtr<TFollowerPinger>;

    const TDistributedElectionManagerConfigPtr Config_;
    const IInvokerPtr ControlInvoker_;
    const IElectionCallbacksPtr ElectionCallbacks_;
    const IServerPtr RpcServer_;

    TCellManagerPtr CellManager_;

    EPeerState State_ = EPeerState::Stopped;

    // Voting parameters.
    TPeerId VoteId_ = InvalidPeerId;
    TEpochId VoteEpochId_;

    // Epoch parameters.
    TEpochContextPtr EpochContext_;
    TAtomicObject<TEpochContextPtr> TentativeEpochContext_;
    IInvokerPtr EpochControlInvoker_;

    TPeerIdSet AliveFollowers_; // actually, includes the leader, too
    TPeerIdSet AlivePeers_; // additionally includes non-voting peers
    TPeerIdSet PotentialPeers_;

    TLease LeaderPingLease_;
    TFollowerPingerPtr FollowerPinger_;


    // Corresponds to #ControlInvoker_.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);

    void Reset();
    void CancelContext();

    void OnLeaderPingLeaseExpired();

    bool CheckQuorum();

    //! Returns true iff this peer is voting.
    bool IsVotingPeer() const;
    // Returns true iff the specified peer is voting.
    bool IsVotingPeer(TPeerId peerId) const;

    void StartVotingRound();
    void ContinueVoting(TPeerId voteId, TEpochId voteEpochId);
    void StartVoting();

    void StartLeading();
    void StartFollowing(TPeerId leaderId, TEpochId epoch);

    void StopLeading(const TError& error);
    void StopFollowing(const TError& error);
    void StopVoting(const TError& error);

    void InitEpochContext(TPeerId leaderId, TEpochId epoch);
    void SetState(EPeerState newState);

    void FireAlivePeerSetChanged();
};

DEFINE_REFCOUNTED_TYPE(TDistributedElectionManager)

////////////////////////////////////////////////////////////////////////////////

// Also pings observers.
class TDistributedElectionManager::TFollowerPinger
    : public TRefCounted
{
public:
    explicit TFollowerPinger(TDistributedElectionManagerPtr owner)
        : Owner_(std::move(owner))
        , Logger(Owner_->Logger)
        , SendPingsExecutor_(New<TPeriodicExecutor>(
            Owner_->EpochControlInvoker_,
            BIND(&TFollowerPinger::SendPings, MakeWeak(this)),
            Owner_->Config_->FollowerPingPeriod))
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);
        SendPingsExecutor_->Start();
    }

private:
    const TDistributedElectionManagerPtr Owner_;
    const NLogging::TLogger Logger;
    const TPeriodicExecutorPtr SendPingsExecutor_;

    // Ping sessions are in progress for peers [0, TotalPeerCount_) (except for self).
    int TotalPeerCount_ = 0;

    void SendPings()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        const auto& cellManager = Owner_->CellManager_;
        for (auto id = TotalPeerCount_; id < cellManager->GetTotalPeerCount(); ++id) {
            SendPing(id);
        }
        TotalPeerCount_ = cellManager->GetTotalPeerCount();
    }

    void SendPing(TPeerId peerId)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (peerId == Owner_->CellManager_->GetSelfPeerId()) {
            return;
        }

        auto channel = Owner_->CellManager_->GetPeerChannel(peerId);
        if (!channel) {
            SchedulePing(peerId);
            return;
        }

        YT_LOG_DEBUG("Sending ping to follower (PeerId: %v)", peerId);

        TElectionServiceProxy proxy(channel);
        auto req = proxy.PingFollower();
        req->SetTimeout(Owner_->Config_->FollowerPingRpcTimeout);
        req->set_leader_id(Owner_->CellManager_->GetSelfPeerId());
        ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);

        req->Invoke().Subscribe(
            BIND(&TFollowerPinger::OnPingResponse, MakeWeak(this), peerId)
                .Via(Owner_->EpochControlInvoker_));
    }

    void SchedulePing(TPeerId id)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        TDelayedExecutor::Submit(
            BIND(&TFollowerPinger::SendPing, MakeWeak(this), id)
                .Via(Owner_->EpochControlInvoker_),
            Owner_->Config_->FollowerPingPeriod);
    }

    void OnPingResponse(TPeerId id, const TElectionServiceProxy::TErrorOrRspPingFollowerPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);
        YT_VERIFY(Owner_->State_ == EPeerState::Leading);

        if (id >= TotalPeerCount_) {
            return;
        }

        if (rspOrError.IsOK()) {
            OnPingResponseSuccess(id, rspOrError.Value());
        } else {
            OnPingResponseFailure(id, rspOrError);
        }
    }

    void OnPingResponseSuccess(TPeerId id, TElectionServiceProxy::TRspPingFollowerPtr rsp)
    {
        YT_LOG_DEBUG("Ping reply from follower (PeerId: %v)", id);

        const auto votingPeer = Owner_->IsVotingPeer(id);

        if (Owner_->PotentialPeers_.contains(id)) {
            YT_LOG_INFO("%v is up, first success (PeerId: %v)",
                votingPeer ? "Follower" : "Observer",
                id);
            YT_VERIFY(Owner_->PotentialPeers_.erase(id) == 1);
        } else {
            if (votingPeer) {
                if (!Owner_->AliveFollowers_.contains(id)) {
                    YT_LOG_INFO("Follower is up (PeerId: %v)", id);
                    YT_VERIFY(Owner_->AliveFollowers_.insert(id).second);
                    // Peers are a superset of followers.
                    YT_VERIFY(Owner_->AlivePeers_.insert(id).second);
                    Owner_->FireAlivePeerSetChanged();
                }
            } else {
                if (!Owner_->AlivePeers_.contains(id)) {
                    YT_LOG_INFO("Observer is up (PeerId: %v)", id);
                    YT_VERIFY(Owner_->AlivePeers_.insert(id).second);
                    Owner_->FireAlivePeerSetChanged();
                }
            }
        }

        SchedulePing(id);
    }

    void OnPingResponseFailure(TPeerId id, const TError& error)
    {
        auto votingPeer = Owner_->IsVotingPeer(id);

        auto code = error.GetCode();
        if (code == NElection::EErrorCode::InvalidState ||
            code == NElection::EErrorCode::InvalidLeader ||
            code == NElection::EErrorCode::InvalidEpoch)
        {
            // These errors are possible during grace period.
            if (Owner_->PotentialPeers_.contains(id)) {
                 if (TInstant::Now() > Owner_->EpochContext_->StartTime + Owner_->Config_->FollowerGraceTimeout) {
                    YT_LOG_WARNING(error, "Error pinging %v, no success within grace period, considered down (PeerId: %v)",
                        votingPeer ? "follower" : "observer",
                        id);
                    Owner_->PotentialPeers_.erase(id);
                    Owner_->AliveFollowers_.erase(id);
                    if (Owner_->AlivePeers_.erase(id) > 0) {
                        Owner_->FireAlivePeerSetChanged();
                    }
                } else {
                    YT_LOG_INFO(error, "Error pinging %v, will retry later (PeerId: %v)",
                        votingPeer ? "follower": "observer",
                        id);
                }
            } else {
                if (votingPeer) {
                    if (Owner_->AliveFollowers_.erase(id) > 0) {
                        YT_LOG_WARNING(error, "Error pinging follower, considered down (PeerId: %v)",
                            id);
                        YT_VERIFY(Owner_->AlivePeers_.erase(id));
                        Owner_->FireAlivePeerSetChanged();
                    }
                } else {
                    if (Owner_->AlivePeers_.erase(id) > 0) {
                        YT_LOG_WARNING(error, "Error pinging observer, considered down (PeerId: %v)",
                            id);
                        Owner_->FireAlivePeerSetChanged();
                    }
                }
            }
        } else {
            if (votingPeer) {
                if (Owner_->AliveFollowers_.erase(id) > 0) {
                    YT_LOG_WARNING(error, "Error pinging follower, considered down (PeerId: %v)",
                        id);
                    Owner_->PotentialPeers_.erase(id);
                    YT_VERIFY(Owner_->AlivePeers_.erase(id) > 0);
                    Owner_->FireAlivePeerSetChanged();
                }
            } else {
                if (Owner_->AlivePeers_.erase(id) > 0) {
                    YT_LOG_WARNING(error, "Error pinging observer, considered down (PeerId: %v)",
                        id);
                    Owner_->PotentialPeers_.erase(id);
                    Owner_->FireAlivePeerSetChanged();
                }
            }
        }

        if (votingPeer && !Owner_->CheckQuorum()) {
            return;
        }

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
        : Owner_(std::move(owner))
        , Logger(NLogging::TLogger(Owner_->Logger)
            .AddTag("RoundId: %v, VoteEpochId: %v",
                TGuid::Create(),
                Owner_->VoteEpochId_))
    { }

    void Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);
        YT_VERIFY(Owner_->State_ == EPeerState::Voting);

        const auto& cellManager = Owner_->CellManager_;

        YT_LOG_DEBUG("New voting round started");

        if (Owner_->IsVotingPeer()) {
            ProcessVote(
                cellManager->GetSelfPeerId(),
                TStatus(
                    Owner_->State_,
                    Owner_->VoteId_,
                    Owner_->ElectionCallbacks_->GetPriority(),
                    Owner_->VoteEpochId_));
        }

        std::vector<TFuture<void>> asyncResults;
        for (TPeerId id = 0; id < cellManager->GetTotalPeerCount(); ++id) {
            if (id == cellManager->GetSelfPeerId()) {
                continue;
            }

            auto channel = cellManager->GetPeerChannel(id);
            if (!channel) {
                continue;
            }

            if (!Owner_->IsVotingPeer(id)) {
                continue;
            }

            TElectionServiceProxy proxy(channel);
            proxy.SetDefaultTimeout(Owner_->Config_->ControlRpcTimeout);

            auto req = proxy.GetStatus();
            asyncResults.push_back(
                req->Invoke().Apply(
                    BIND(&TVotingRound::OnResponse, MakeStrong(this), id)
                        .AsyncVia(Owner_->EpochControlInvoker_)));
        }

        Combine(asyncResults).Subscribe(
            BIND(&TVotingRound::OnComplete, MakeStrong(this))
                .Via(Owner_->EpochControlInvoker_));
    }

private:
    const TDistributedElectionManagerPtr Owner_;
    const NLogging::TLogger Logger;

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
            TEpochId voteEpochId = TEpochId())
            : State(state)
            , VoteId(vote)
            , Priority(priority)
            , VoteEpochId(voteEpochId)
        { }
    };

    THashMap<TPeerId, TStatus> StatusTable_;
    bool Finished_ = false;


    void ProcessVote(TPeerId id, const TStatus& status)
    {
        YT_LOG_DEBUG("Vote received (PeerId: %v, State: %v, VoteId: %v, Priority: %v)",
            id,
            status.State,
            status.VoteId,
            Owner_->ElectionCallbacks_->FormatPriority(status.Priority));

        YT_VERIFY(id != InvalidPeerId);
        StatusTable_[id] = status;

        for (const auto& [id, status] : StatusTable_) {
            if (CheckForLeader(id, status)) {
                break;
            }
        }
    }

    void OnResponse(TPeerId id, const TElectionServiceProxy::TErrorOrRspGetStatusPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (Finished_) {
            return;
        }

        if (!rspOrError.IsOK()) {
            YT_LOG_INFO(rspOrError, "Error requesting status from peer (PeerId: %v)",
                id);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto state = FromProto<EPeerState>(rsp->state());
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
            candidateId == Owner_->CellManager_->GetSelfPeerId()
            ? Owner_->VoteEpochId_
            : candidateStatus.VoteEpochId;

        // Count votes (including self) and quorum.
        int voteCount = CountVotesFor(candidateId, candidateEpochId);
        int quorumCount = Owner_->CellManager_->GetQuorumPeerCount();

        // Check for quorum.
        if (voteCount < quorumCount) {
            return false;
        }

        YT_LOG_DEBUG("Candidate has quorum (PeerId: %v, VoteCount: %v, QuorumCount: %v)",
            candidateId,
            voteCount,
            quorumCount);

        Finished_ = true;

        // Become a leader or a follower.
        if (candidateId == Owner_->CellManager_->GetSelfPeerId()) {
            Owner_->EpochControlInvoker_->Invoke(BIND(
                &TDistributedElectionManager::StartLeading,
                Owner_));
        } else {
            Owner_->EpochControlInvoker_->Invoke(BIND(
                &TDistributedElectionManager::StartFollowing,
                Owner_,
                candidateId,
                candidateStatus.VoteEpochId));
        }

        return true;
    }

    int CountVotesFor(TPeerId candidateId, TEpochId epochId) const
    {
        int result = 0;
        for (const auto& pair : StatusTable_) {
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

        if (candidateId == Owner_->CellManager_->GetSelfPeerId()) {
            // Check that we're voting.
            YT_VERIFY(candidateStatus.State == EPeerState::Voting);
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
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (Finished_) {
            return;
        }

        YT_LOG_DEBUG("Voting round completed");

        // Choose the best vote.
        std::optional<TStatus> bestCandidate;
        for (const auto& [_, currentCandidate] : StatusTable_) {
            if (StatusTable_.find(currentCandidate.VoteId) != StatusTable_.end() &&
                (!bestCandidate || IsBetterCandidate(currentCandidate, *bestCandidate)))
            {
                bestCandidate = currentCandidate;
            }
        }

        if (bestCandidate) {
            // Extract the status of the best candidate.
            // His status must be present in the table by the above checks.
            const auto& candidateStatus = StatusTable_[bestCandidate->VoteId];
            Owner_->ContinueVoting(candidateStatus.VoteId, candidateStatus.VoteEpochId);
        } else {
            Owner_->StartVoting();
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
            .AddTag("CellId: %v", cellManager->GetCellId()),
        cellManager->GetCellId())
    , Config_(std::move(config))
    , ControlInvoker_(std::move(controlInvoker))
    , ElectionCallbacks_(std::move(electionCallbacks))
    , RpcServer_(std::move(rpcServer))
    , CellManager_(std::move(cellManager))
{
    YT_VERIFY(Config_);
    YT_VERIFY(ControlInvoker_);
    YT_VERIFY(ElectionCallbacks_);
    VERIFY_INVOKER_THREAD_AFFINITY(ControlInvoker_, ControlThread);

    RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
    RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));
}

void TDistributedElectionManager::Initialize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    RpcServer_->RegisterService(this);

    YT_LOG_INFO("Election instance initialized");
}

void TDistributedElectionManager::Finalize()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_LOG_INFO("Election instance is finalizing");

    Abandon(TError("Election instance is finalizing"));

    RpcServer_->UnregisterService(this);
}

void TDistributedElectionManager::Participate()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State_) {
        case EPeerState::Stopped:
            StartVoting();
            break;

        case EPeerState::Voting:
            break;

        case EPeerState::Leading:
            StopLeading(TError("Leader is requested to participate in re-elections"));
            StartVoting();
            break;

        case EPeerState::Following:
            StopFollowing(TError("Follower is requested to participate in re-elections"));
            StartVoting();
            break;

        default:
            YT_ABORT();
    }
}

void TDistributedElectionManager::Abandon(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State_) {
        case EPeerState::Stopped:
            break;

        case EPeerState::Voting:
            StopVoting(error);
            break;

        case EPeerState::Leading:
            StopLeading(error);
            break;

        case EPeerState::Following:
            StopFollowing(error);
            break;

        default:
            YT_ABORT();
    }
}

void TDistributedElectionManager::ReconfigureCell(TCellManagerPtr cellManager)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(cellManager);
    YT_VERIFY(CellManager_->GetCellId() == cellManager->GetCellId());

    CellManager_ = std::move(cellManager);
    Abandon(TError("Cell reconfigured"));

    YT_LOG_INFO("Peer reconfigured");
}

TYsonProducer TDistributedElectionManager::GetMonitoringProducer()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return BIND([=, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
        auto epochContext = TentativeEpochContext_.Load();
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("state").Value(State_)
                .DoIf(epochContext.operator bool(), [&] (TFluentMap fluent) {
                    fluent
                        .Item("self_peer_id").Value(epochContext->CellManager->GetSelfPeerId())
                        .Item("peers").BeginList()
                            .DoFor(0, epochContext->CellManager->GetTotalPeerCount(), [=] (TFluentList fluent, TPeerId id) {
                                fluent.Item().Value(epochContext->CellManager->GetPeerConfig(id));
                            })
                        .EndList()
                        .Item("leader_id").Value(epochContext->LeaderId)
                        .Item("epoch_id").Value(epochContext->EpochId);
                })
                .Item("vote_id").Value(VoteId_)
            .EndMap();
    });
}

void TDistributedElectionManager::Reset()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Stopped);

    CancelContext();

    VoteId_ = InvalidPeerId;
    AliveFollowers_.clear();
    AlivePeers_.clear();
    PotentialPeers_.clear();
    TLeaseManager::CloseLease(LeaderPingLease_);
    LeaderPingLease_.Reset();
}

void TDistributedElectionManager::CancelContext()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (EpochContext_) {
        EpochContext_->CancelableContext->Cancel(TError("Election epoch canceled"));
    }
    EpochContext_.Reset();
}

void TDistributedElectionManager::OnLeaderPingLeaseExpired()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(State_ == EPeerState::Following);
    StopFollowing(TError("No recurrent ping from leader within timeout"));
}

bool TDistributedElectionManager::CheckQuorum()
{
    if (AliveFollowers_.size() >= CellManager_->GetQuorumPeerCount()) {
        return true;
    }

    StopLeading(TError("Quorum is lost"));

    return false;
}

bool TDistributedElectionManager::IsVotingPeer() const
{
    const auto& config = CellManager_->GetSelfConfig();
    return config.Voting;
}

bool TDistributedElectionManager::IsVotingPeer(TPeerId peerId) const
{
    const auto& config = CellManager_->GetPeerConfig(peerId);
    return config.Voting;
}

void TDistributedElectionManager::ContinueVoting(TPeerId voteId, TEpochId voteEpoch)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Voting);
    VoteId_ = voteId;
    VoteEpochId_ = voteEpoch;

    YT_LOG_DEBUG("Voting for another candidate (VoteId: %v, VoteEpochId: %v)",
        VoteId_,
        VoteEpochId_);

    StartVotingRound();
}

void TDistributedElectionManager::StartVoting()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CancelContext();

    EpochContext_ = New<TEpochContext>();
    TentativeEpochContext_.Store(EpochContext_);
    EpochContext_->CellManager = CellManager_;
    
    EpochControlInvoker_ = EpochContext_->CancelableContext->CreateInvoker(ControlInvoker_);

    SetState(EPeerState::Voting);
    VoteEpochId_ = TGuid::Create();

    if (IsVotingPeer()) {
        VoteId_ = CellManager_->GetSelfPeerId();
        YT_LOG_DEBUG("Voting for self (VoteId_: %v, VoteEpochId_: %v)",
            VoteId_,
            VoteEpochId_);
    } else {
        VoteId_ = InvalidPeerId;
        YT_LOG_DEBUG("Voting for nobody (VoteEpochId_: %v)",
            VoteEpochId_);
    }

    StartVotingRound();
}

void TDistributedElectionManager::StartVotingRound()
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Voting);

    auto round = New<TVotingRound>(this);
    TDelayedExecutor::Submit(
        BIND(&TVotingRound::Run, round)
            .Via(EpochControlInvoker_),
        Config_->VotingRoundPeriod);
}

void TDistributedElectionManager::StartLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Leading);
    YT_VERIFY(VoteId_ == CellManager_->GetSelfPeerId());

    // Initialize followers state.
    for (TPeerId id = 0; id < CellManager_->GetTotalPeerCount(); ++id) {
        PotentialPeers_.insert(id);
        AlivePeers_.insert(id);
        if (IsVotingPeer(id)) {
            AliveFollowers_.insert(id);
        }
    }

    InitEpochContext(CellManager_->GetSelfPeerId(), VoteEpochId_);

    // Send initial pings.
    YT_VERIFY(!FollowerPinger_);
    FollowerPinger_ = New<TFollowerPinger>(this);
    FollowerPinger_->Run();

    YT_LOG_INFO("Started leading (EpochId: %v)",
        EpochContext_->EpochId);

    BIND(&IElectionCallbacks::OnStartLeading, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(EpochContext_);
}

void TDistributedElectionManager::StartFollowing(
    TPeerId leaderId,
    TEpochId epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Following);
    VoteId_ = leaderId;
    VoteEpochId_ = epochId;

    InitEpochContext(leaderId, epochId);

    LeaderPingLease_ = TLeaseManager::CreateLease(
        Config_->LeaderPingTimeout,
        BIND(&TDistributedElectionManager::OnLeaderPingLeaseExpired, MakeWeak(this))
            .Via(EpochControlInvoker_));

    YT_LOG_INFO("Started following (LeaderId: %v, EpochId: %v)",
        EpochContext_->LeaderId,
        EpochContext_->EpochId);

    BIND(&IElectionCallbacks::OnStartFollowing, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(EpochContext_);
}

void TDistributedElectionManager::StopLeading(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Leading);

    YT_LOG_INFO(error, "Stopped leading (EpochId: %v)",
        EpochContext_->EpochId);

    BIND(&IElectionCallbacks::OnStopLeading, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(error);

    YT_VERIFY(FollowerPinger_);
    FollowerPinger_.Reset();

    Reset();
}

void TDistributedElectionManager::StopFollowing(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Following);

    YT_LOG_INFO(error, "Stopped following (LeaderId: %v, EpochId: %v)",
        EpochContext_->LeaderId,
        EpochContext_->EpochId);

    BIND(&IElectionCallbacks::OnStopFollowing, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(error);

    Reset();
}

void TDistributedElectionManager::StopVoting(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Voting);

    YT_LOG_INFO(error, "Voting stopped (EpochId: %v)",
        EpochContext_->EpochId);

    BIND(&IElectionCallbacks::OnStopVoting, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(error);

    Reset();
}

void TDistributedElectionManager::InitEpochContext(TPeerId leaderId, TEpochId epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EpochContext_->LeaderId = leaderId;
    EpochContext_->EpochId = epochId;
    EpochContext_->StartTime = TInstant::Now();
}

void TDistributedElectionManager::SetState(EPeerState newState)
{
    if (newState == State_)
        return;

    // This generic message logged to simplify tracking state changes.
    YT_LOG_INFO("State changed: %v -> %v",
        State_,
        newState);
    State_ = newState;
}

void TDistributedElectionManager::FireAlivePeerSetChanged()
{
    BIND(&IElectionCallbacks::OnAlivePeerSetChanged, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(AlivePeers_);
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

    if (State_ != EPeerState::Following) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidState,
            "Received ping in invalid state: expected %Qlv, actual %Qlv",
            EPeerState::Following,
            State_);
    }

    if (epochId != EpochContext_->EpochId) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidEpoch,
            "Received ping with invalid epoch: expected %v, received %v",
            EpochContext_->EpochId,
            epochId);
    }

    if (leaderId != EpochContext_->LeaderId) {
        THROW_ERROR_EXCEPTION(
            NElection::EErrorCode::InvalidLeader,
            "Ping from an invalid leader: expected %v, received %v",
            EpochContext_->LeaderId,
            leaderId);
    }

    TLeaseManager::RenewLease(LeaderPingLease_);

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

    auto priority = ElectionCallbacks_->GetPriority();

    response->set_state(static_cast<int>(State_));
    response->set_vote_id(VoteId_);
    response->set_priority(priority);
    ToProto(response->mutable_vote_epoch_id(), VoteEpochId_);
    response->set_self_id(CellManager_->GetSelfPeerId());

    context->SetResponseInfo("State: %v, VoteId_: %v, Priority: %v, VoteEpochId_: %v",
        State_,
        VoteId_,
        ElectionCallbacks_->FormatPriority(priority),
        VoteEpochId_);

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
        std::move(config),
        std::move(cellManager),
        std::move(controlInvoker),
        std::move(electionCallbacks),
        std::move(rpcServer));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
