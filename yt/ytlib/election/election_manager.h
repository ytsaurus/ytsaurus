#pragma once

#include "common.h"
#include "leader_lookup.h"
#include "election_manager_rpc.h"

#include "../meta_state/cell_manager.h"
#include "../misc/delayed_invoker.h"
#include "../misc/thread_affinity.h"
#include "../actions/invoker.h"
#include "../rpc/client.h"
#include "../rpc/server.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

struct IElectionCallbacks
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IElectionCallbacks> TPtr;

    virtual void OnStartLeading(const TEpoch& epoch) = 0;
    virtual void OnStopLeading() = 0;
    virtual void OnStartFollowing(TPeerId leaderId, const TEpoch& epoch) = 0;
    virtual void OnStopFollowing() = 0;

    virtual TPeerPriority GetPriority() = 0;
    virtual Stroka FormatPriority(TPeerPriority priority) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TElectionManager
    : public NRpc::TServiceBase
{
public:
    typedef TIntrusivePtr<TElectionManager> TPtr;

    struct TConfig
    {
        static const TDuration RpcTimeout;
        static const TDuration FollowerPingInterval;
        static const TDuration FollowerPingTimeout;
        static const TDuration ReadyToFollowTimeout;
        static const TDuration PotentialFollowerTimeout;
        static const TDuration VotingPeriod;

        TConfig();
    };

    TElectionManager(
        const TConfig& config,
        NMetaState::TCellManager::TPtr cellManager,
        IInvoker::TPtr controlInvoker,
        IElectionCallbacks::TPtr electionCallbacks,
        NRpc::TServer::TPtr server);

    ~TElectionManager();

    /*!
     * \note Thread affinity: any
     */
    void Start();

    /*!
     * \note Thread affinity: any
     */
    void Stop();

    /*!
     * \note Thread affinity: any
     */
    void Restart();
    
private:
    typedef TElectionManager TThis;
    typedef TElectionManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;
    typedef NRpc::TTypedServiceException<EErrorCode> TServiceException;

    class TVotingRound;
    class TFollowerPinger;

    TProxy::EState State;
    
    // Voting parameters.
    TPeerId VoteId;
    TEpoch VoteEpoch;

    // Epoch parameters.
    TPeerId LeaderId;
    TGuid Epoch;
    TInstant EpochStart;
    TCancelableInvoker::TPtr ControlEpochInvoker;
    
    typedef yhash_set<TPeerId> TPeerSet;
    TPeerSet AliveFollowers;
    TPeerSet PotentialFollowers;

    TDelayedInvoker::TCookie PingTimeoutCookie;
    TIntrusivePtr<TFollowerPinger> FollowerPinger;

    TConfig Config;
    NMetaState::TCellManager::TPtr CellManager;
    IInvoker::TPtr ControlInvoker;
    IElectionCallbacks::TPtr ElectionCallbacks;

    // Corresponds to #ControlInvoker.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    RPC_SERVICE_METHOD_DECL(NElection::NProto, PingFollower);
    RPC_SERVICE_METHOD_DECL(NElection::NProto, GetStatus);

    void RegisterMethods();

    void Reset();
    void OnLeaderPingTimeout();

    void DoStart();
    void DoStop();

    void StartVotingRound();
    void StartVoteFor(TPeerId voteId, const TEpoch& voteEpoch);
    void StartVoteForSelf();

    void StartLeading();
    void StartFollowing(TPeerId leaderId, const TEpoch& epoch);
    void StopLeading();
    void StopFollowing();

    void StartEpoch(TPeerId leaderId, const TEpoch& epoch);
    void StopEpoch();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
