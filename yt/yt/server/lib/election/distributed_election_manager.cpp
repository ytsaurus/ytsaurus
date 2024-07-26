#include "election_manager.h"
#include "private.h"
#include "public.h"
#include "config.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>
#include <yt/yt/ytlib/election/election_service_proxy.h>

#include <yt/yt/ytlib/election/proto/election_service.pb.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/lease_manager.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NElection {

using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;
using namespace NRpc;

using NYT::ToProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto MonitoringUpdatePeriod = TDuration::Seconds(1);

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
        IServerPtr rpcServer,
        IAuthenticatorPtr authenticator);

    void Initialize() override;
    void Finalize() override;

    void Participate() override;
    TFuture<void> Abandon(const TError& error) override;
    void ReconfigureCell(TCellManagerPtr cellManager) override;

    TYsonProducer GetMonitoringProducer() override;
    TPeerIdSet GetAlivePeerIds() override;

    TCellManagerPtr GetCellManager() override;

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
    int VoteId_ = InvalidPeerId;
    TEpochId VoteEpochId_;

    // Epoch parameters.
    TEpochContextPtr EpochContext_;
    IInvokerPtr EpochControlInvoker_;

    TPeerIdSet AliveFollowerIds_; // actually, includes the leader, too
    TPeerIdSet AlivePeerIds_; // additionally includes non-voting peers
    TPeerIdSet PotentialPeerIds_;

    TLease LeaderPingLease_;
    TFollowerPingerPtr FollowerPinger_;


    // Corresponds to #ControlInvoker_.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, Discombobulate);

    void Reset();
    void CancelContext();

    void OnLeaderPingLeaseExpired();

    bool CheckQuorum();

    //! Returns true iff this peer is voting.
    bool IsVotingPeer() const;
    // Returns true iff the specified peer is voting.
    bool IsVotingPeer(int peerId) const;

    void StartVotingRound();
    void ContinueVoting(int voteId, TEpochId voteEpochId);
    void StartVoting();

    TFuture<void> StartLeading();
    TFuture<void> StartFollowing(int leaderId, TEpochId epoch);

    TFuture<void> StopLeading(const TError& error);
    TFuture<void> StopFollowing(const TError& error);
    TFuture<void> StopVoting(const TError& error);
    TFuture<void> Discombobulate(i64 leaderSequenceNumber);

    void InitEpochContext(int leaderId, TEpochId epoch);
    void SetState(EPeerState newState);
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

    void SendPing(int peerId)
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

    void SchedulePing(int id)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        TDelayedExecutor::Submit(
            BIND(&TFollowerPinger::SendPing, MakeWeak(this), id)
                .Via(Owner_->EpochControlInvoker_),
            Owner_->Config_->FollowerPingPeriod);
    }

    void OnPingResponse(int id, const TElectionServiceProxy::TErrorOrRspPingFollowerPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);
        YT_VERIFY(Owner_->State_ == EPeerState::Leading);

        if (id >= TotalPeerCount_) {
            return;
        }

        if (rspOrError.IsOK()) {
            OnPingResponseSuccess(id);
        } else {
            OnPingResponseFailure(id, rspOrError);
        }
    }

    void OnPingResponseSuccess(int id)
    {
        YT_LOG_DEBUG("Ping reply from follower (PeerId: %v)", id);

        const auto votingPeer = Owner_->IsVotingPeer(id);

        if (Owner_->PotentialPeerIds_.contains(id)) {
            YT_LOG_INFO("%v is up, first success (PeerId: %v)",
                votingPeer ? "Follower" : "Observer",
                id);
            YT_VERIFY(Owner_->PotentialPeerIds_.erase(id) == 1);
        } else {
            if (votingPeer) {
                if (!Owner_->AliveFollowerIds_.contains(id)) {
                    YT_LOG_INFO("Follower is up (PeerId: %v)", id);
                    YT_VERIFY(Owner_->AliveFollowerIds_.insert(id).second);
                    // Peers are a superset of followers.
                    YT_VERIFY(Owner_->AlivePeerIds_.insert(id).second);
                }
            } else {
                if (!Owner_->AlivePeerIds_.contains(id)) {
                    YT_LOG_INFO("Observer is up (PeerId: %v)", id);
                    YT_VERIFY(Owner_->AlivePeerIds_.insert(id).second);
                }
            }
        }

        SchedulePing(id);
    }

    void OnPingResponseFailure(int id, const TError& error)
    {
        auto votingPeer = Owner_->IsVotingPeer(id);

        auto code = error.GetCode();
        if (code == NElection::EErrorCode::InvalidState ||
            code == NElection::EErrorCode::InvalidLeader ||
            code == NElection::EErrorCode::InvalidEpoch ||
            code == NRpc::EErrorCode::NoSuchService ||
            code == NRpc::EErrorCode::NoSuchRealm ||
            code == NYTree::EErrorCode::ResolveError)
        {
            // These errors are possible during grace period.
            if (Owner_->PotentialPeerIds_.contains(id)) {
                if (TInstant::Now() > Owner_->EpochContext_->StartTime + Owner_->Config_->FollowerGraceTimeout) {
                    YT_LOG_WARNING(error, "Error pinging %v, no success within grace period, considered down (PeerId: %v)",
                        votingPeer ? "follower" : "observer",
                        id);
                    Owner_->PotentialPeerIds_.erase(id);
                    Owner_->AliveFollowerIds_.erase(id);
                    Owner_->AlivePeerIds_.erase(id);
                } else {
                    YT_LOG_INFO(error, "Error pinging %v, will retry later (PeerId: %v)",
                        votingPeer ? "follower": "observer",
                        id);
                }
            } else {
                if (votingPeer) {
                    if (Owner_->AliveFollowerIds_.erase(id) > 0) {
                        YT_LOG_WARNING(error, "Error pinging follower, considered down (PeerId: %v)",
                            id);
                        YT_VERIFY(Owner_->AlivePeerIds_.erase(id));
                    }
                } else {
                    if (Owner_->AlivePeerIds_.erase(id) > 0) {
                        YT_LOG_WARNING(error, "Error pinging observer, considered down (PeerId: %v)",
                            id);
                    }
                }
            }
        } else {
            if (votingPeer) {
                if (Owner_->AliveFollowerIds_.erase(id) > 0) {
                    YT_LOG_WARNING(error, "Error pinging follower, considered down (PeerId: %v)",
                        id);
                    Owner_->PotentialPeerIds_.erase(id);
                    YT_VERIFY(Owner_->AlivePeerIds_.erase(id) > 0);
                }
            } else {
                if (Owner_->AlivePeerIds_.erase(id) > 0) {
                    YT_LOG_WARNING(error, "Error pinging observer, considered down (PeerId: %v)",
                        id);
                    Owner_->PotentialPeerIds_.erase(id);
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
        , Logger(Owner_->Logger().WithTag("RoundId: %v, VoteEpochId: %v",
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
        for (int id = 0; id < cellManager->GetTotalPeerCount(); ++id) {
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

        AllSucceeded(asyncResults).Subscribe(
            BIND(&TVotingRound::OnComplete, MakeStrong(this))
                .Via(Owner_->EpochControlInvoker_));
    }

private:
    const TDistributedElectionManagerPtr Owner_;
    const NLogging::TLogger Logger;

    struct TStatus
    {
        EPeerState State;
        int VoteId;
        TPeerPriority Priority;
        TEpochId VoteEpochId;

        TStatus(
            EPeerState state = EPeerState::Stopped,
            int vote = InvalidPeerId,
            TPeerPriority priority = {-1, -1},
            TEpochId voteEpochId = TEpochId())
            : State(state)
            , VoteId(vote)
            , Priority(priority)
            , VoteEpochId(voteEpochId)
        { }
    };

    THashMap<int, TStatus> StatusTable_;
    bool Finished_ = false;


    void ProcessVote(int id, const TStatus& status)
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

    void OnResponse(int id, const TElectionServiceProxy::TErrorOrRspGetStatusPtr& rspOrError)
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
        auto priority = FromProto<TPeerPriority>(rsp->priority());
        auto epochId = FromProto<TEpochId>(rsp->vote_epoch_id());
        ProcessVote(id, TStatus(state, voteId, priority, epochId));
    }

    bool CheckForLeader(int candidateId, const TStatus& candidateStatus)
    {
        if (!IsFeasibleLeader(candidateId, candidateStatus)) {
            return false;
        }

        // Count votes (including self) and quorum.
        int voteCount = CountVotesFor(candidateId, candidateStatus.VoteEpochId);
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
            YT_UNUSED_FUTURE(BIND(&TDistributedElectionManager::StartLeading, Owner_)
                .AsyncVia(Owner_->EpochControlInvoker_)
                .Run());
        } else {
            YT_UNUSED_FUTURE(BIND(&TDistributedElectionManager::StartFollowing, Owner_)
                .AsyncVia(Owner_->EpochControlInvoker_)
                .Run(candidateId, candidateStatus.VoteEpochId));
        }

        return true;
    }

    int CountVotesFor(int candidateId, TEpochId epochId) const
    {
        int result = 0;
        for (const auto& [peerId, peerStatus] : StatusTable_) {
            if (peerStatus.VoteId == candidateId && peerStatus.VoteEpochId == epochId) {
                ++result;
            }
        }
        return result;
    }

    bool IsFeasibleLeader(int candidateId, const TStatus& candidateStatus) const
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

    using TCandidate = std::pair<int, TPeerPriority>;

    // Compare votes lexicographically by (priority, id).
    static bool IsBetterCandidate(const TCandidate& lhs, const TCandidate& rhs)
    {
        if (lhs.second != rhs.second) {
            return lhs.second > rhs.second;
        }

        return lhs.first < rhs.first;
    }

    void OnComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (Finished_) {
            return;
        }

        YT_LOG_DEBUG("Voting round completed, choosing the best vote");

        // Choose the best vote.
        std::optional<TCandidate> bestCandidate;
        for (const auto& [currentCandidateId, currentCandidateStatus] : StatusTable_) {
            YT_LOG_DEBUG("Considering peer (PeerId: %v, State: %v, VoteId: %v, Priority: %v, VoteEpochId: %v)",
                currentCandidateId,
                currentCandidateStatus.State,
                currentCandidateStatus.VoteId,
                Owner_->ElectionCallbacks_->FormatPriority(currentCandidateStatus.Priority),
                currentCandidateStatus.VoteEpochId);
            auto currentCandidate = std::pair(currentCandidateId, currentCandidateStatus.Priority);
            if (!bestCandidate || IsBetterCandidate(currentCandidate, *bestCandidate)) {
                bestCandidate = currentCandidate;
                YT_LOG_DEBUG("Updated best candidate (PeerId: %v, Priority %v)",
                    currentCandidateId,
                    currentCandidateStatus.Priority);
            }
        }

        if (bestCandidate) {
            // Extract the status of the best candidate.
            // His status must be present in the table by the above checks.
            const auto& candidateStatus = StatusTable_[bestCandidate->first];
            Owner_->ContinueVoting(bestCandidate->first, candidateStatus.VoteEpochId);
        } else {
            YT_LOG_DEBUG("No suitable candidate to vote for");
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
    IServerPtr rpcServer,
    IAuthenticatorPtr authenticator)
    : TServiceBase(
        controlInvoker,
        TElectionServiceProxy::GetDescriptor(),
        ElectionLogger().WithTag("CellId: %v", cellManager->GetCellId()),
        cellManager->GetCellId(),
        std::move(authenticator))
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
    RegisterMethod(RPC_SERVICE_METHOD_DESC(Discombobulate));
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

    YT_UNUSED_FUTURE(Abandon(TError("Election instance is finalizing")));

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
            YT_UNUSED_FUTURE(StopLeading(TError("Leader is requested to participate in re-elections")));
            StartVoting();
            break;

        case EPeerState::Following:
            YT_UNUSED_FUTURE(StopFollowing(TError("Follower is requested to participate in re-elections")));
            StartVoting();
            break;

        default:
            YT_ABORT();
    }
}

TFuture<void> TDistributedElectionManager::Abandon(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (State_) {
        case EPeerState::Stopped:
            return VoidFuture;

        case EPeerState::Voting:
            return StopVoting(error);

        case EPeerState::Leading:
            return StopLeading(error);

        case EPeerState::Following:
            return StopFollowing(error);

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
    YT_UNUSED_FUTURE(Abandon(TError("Cell reconfigured")));

    YT_LOG_INFO("Peer reconfigured");
}

TYsonProducer TDistributedElectionManager::GetMonitoringProducer()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return
        IYPathService::FromProducer(BIND([=, this, this_ = MakeStrong(this)] (IYsonConsumer* consumer) {
            VERIFY_THREAD_AFFINITY(ControlThread);

            BuildYsonFluently(consumer)
                .BeginMap()
                    .Item("state").Value(State_)
                    .DoIf(EpochContext_.operator bool(), [&] (TFluentMap fluent) {
                        fluent
                            .Item("self_peer_id").Value(EpochContext_->CellManager->GetSelfPeerId())
                            .Item("peers").BeginList()
                                .DoFor(0, EpochContext_->CellManager->GetTotalPeerCount(), [this] (TFluentList fluent, int id) {
                                    fluent.Item().Value(EpochContext_->CellManager->GetPeerConfig(id));
                                })
                            .EndList()
                            .DoIf(EpochContext_->LeaderId != InvalidPeerId, [&] (TFluentMap fluent) {
                                fluent
                                    .Item("leader_id").Value(EpochContext_->LeaderId);
                            })
                            .DoIf(EpochContext_->EpochId != TEpochId(), [&] (TFluentMap fluent) {
                                fluent
                                    .Item("epoch_id").Value(EpochContext_->EpochId);
                            });
                    })
                    .Item("vote_id").Value(VoteId_)
                .EndMap();
        }))
        ->ToProducer(ControlInvoker_, MonitoringUpdatePeriod);
}

TPeerIdSet TDistributedElectionManager::GetAlivePeerIds()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return AlivePeerIds_;
}

TCellManagerPtr TDistributedElectionManager::GetCellManager()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    return CellManager_;
}

void TDistributedElectionManager::Reset()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Stopped);

    CancelContext();

    VoteId_ = InvalidPeerId;
    AliveFollowerIds_.clear();
    AlivePeerIds_.clear();
    PotentialPeerIds_.clear();
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
    YT_UNUSED_FUTURE(StopFollowing(TError("No recurrent ping from leader within timeout")));
}

bool TDistributedElectionManager::CheckQuorum()
{
    if (std::ssize(AliveFollowerIds_) >= CellManager_->GetQuorumPeerCount()) {
        return true;
    }

    YT_UNUSED_FUTURE(StopLeading(TError("Quorum is lost")));

    return false;
}

bool TDistributedElectionManager::IsVotingPeer() const
{
    const auto& config = CellManager_->GetSelfConfig();
    return config->Voting;
}

bool TDistributedElectionManager::IsVotingPeer(int peerId) const
{
    const auto& config = CellManager_->GetPeerConfig(peerId);
    return config->Voting;
}

void TDistributedElectionManager::ContinueVoting(int voteId, TEpochId voteEpoch)
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

    EpochContext_ = New<TEpochContext>(CellManager_);

    EpochControlInvoker_ = EpochContext_->CancelableContext->CreateInvoker(ControlInvoker_);

    SetState(EPeerState::Voting);
    VoteEpochId_ = TGuid::Create();

    if (IsVotingPeer()) {
        VoteId_ = CellManager_->GetSelfPeerId();
        YT_LOG_DEBUG("Voting for self (VoteId: %v, VoteEpochId: %v)",
            VoteId_,
            VoteEpochId_);
    } else {
        VoteId_ = InvalidPeerId;
        YT_LOG_DEBUG("Voting for nobody (VoteEpochId: %v)",
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

TFuture<void> TDistributedElectionManager::StartLeading()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SetState(EPeerState::Leading);
    YT_VERIFY(VoteId_ == CellManager_->GetSelfPeerId());

    // Initialize followers state.
    for (int id = 0; id < CellManager_->GetTotalPeerCount(); ++id) {
        PotentialPeerIds_.insert(id);
        AlivePeerIds_.insert(id);
        if (IsVotingPeer(id)) {
            AliveFollowerIds_.insert(id);
        }
    }

    InitEpochContext(CellManager_->GetSelfPeerId(), VoteEpochId_);

    // Send initial pings.
    YT_VERIFY(!FollowerPinger_);
    FollowerPinger_ = New<TFollowerPinger>(this);
    FollowerPinger_->Run();

    YT_LOG_INFO("Started leading (EpochId: %v)",
        EpochContext_->EpochId);

    return BIND(&IElectionCallbacks::OnStartLeading, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(EpochContext_);
}

TFuture<void> TDistributedElectionManager::StartFollowing(
    int leaderId,
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

    return BIND(&IElectionCallbacks::OnStartFollowing, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(EpochContext_);
}

TFuture<void> TDistributedElectionManager::StopLeading(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Leading);

    YT_LOG_INFO(error, "Stopped leading (EpochId: %v)",
        EpochContext_->EpochId);

    auto future = BIND(&IElectionCallbacks::OnStopLeading, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(error);

    YT_VERIFY(FollowerPinger_);
    FollowerPinger_.Reset();

    Reset();
    return future;
}

TFuture<void> TDistributedElectionManager::StopFollowing(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Following);

    YT_LOG_INFO(error, "Stopped following (LeaderId: %v, EpochId: %v)",
        EpochContext_->LeaderId,
        EpochContext_->EpochId);

    auto future = BIND(&IElectionCallbacks::OnStopFollowing, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(error);

    Reset();
    return future;
}

TFuture<void> TDistributedElectionManager::StopVoting(const TError& error)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Voting);

    YT_LOG_INFO(error, "Voting stopped (EpochId: %v)",
        EpochContext_->EpochId);

    auto future = BIND(&IElectionCallbacks::OnStopVoting, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(error);

    Reset();
    return future;
}

TFuture<void> TDistributedElectionManager::Discombobulate(i64 leaderSequenceNumber)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YT_VERIFY(State_ == EPeerState::Following);

    if (EpochContext_->Discombobulated) {
        YT_LOG_INFO("Already in discombobulated state");
        return VoidFuture;
    }

    YT_LOG_INFO("Entering discombobulated state (EpochId: %v)",
        EpochContext_->EpochId);

    TLeaseManager::RenewLease(
        LeaderPingLease_,
        Config_->DiscombobulatedLeaderPingTimeout);

    return BIND(&IElectionCallbacks::OnDiscombobulate, ElectionCallbacks_)
        .AsyncVia(ControlInvoker_)
        .Run(leaderSequenceNumber);
}

void TDistributedElectionManager::InitEpochContext(int leaderId, TEpochId epochId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EpochContext_->LeaderId = leaderId;
    EpochContext_->EpochId = epochId;
    EpochContext_->StartTime = TInstant::Now();
}

void TDistributedElectionManager::SetState(EPeerState newState)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (newState == State_)
        return;

    // This generic message logged to simplify tracking state changes.
    YT_LOG_INFO("State changed: %v -> %v",
        State_,
        newState);
    State_ = newState;
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
    ToProto(response->mutable_priority(), priority);
    ToProto(response->mutable_vote_epoch_id(), VoteEpochId_);
    response->set_self_id(CellManager_->GetSelfPeerId());

    context->SetResponseInfo("State: %v, VoteId: %v, Priority: %v, VoteEpochId: %v",
        State_,
        VoteId_,
        ElectionCallbacks_->FormatPriority(priority),
        VoteEpochId_);

    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TDistributedElectionManager, Discombobulate)
{
    Y_UNUSED(request, response);
    VERIFY_THREAD_AFFINITY(ControlThread);

    i64 leaderSequenceNumber = request->sequence_number();
    context->SetRequestInfo("LeaderSequenceNumber: %v", leaderSequenceNumber);

    if (IsVotingPeer() || State_ != EPeerState::Following) {
        THROW_ERROR_EXCEPTION(
            NRpc::EErrorCode::Unavailable,
            "Not a following observer");
    }

    WaitFor(Discombobulate(leaderSequenceNumber))
        .ThrowOnError();

    EpochContext_->Discombobulated = true;

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

IElectionManagerPtr CreateDistributedElectionManager(
    TDistributedElectionManagerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr controlInvoker,
    IElectionCallbacksPtr electionCallbacks,
    IServerPtr rpcServer,
    IAuthenticatorPtr authenticator)
{
    return New<TDistributedElectionManager>(
        std::move(config),
        std::move(cellManager),
        std::move(controlInvoker),
        std::move(electionCallbacks),
        std::move(rpcServer),
        std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
