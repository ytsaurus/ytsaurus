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

////////////////////////////////////////////////////////////////////////////////

struct IElectionCallbacks
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IElectionCallbacks> TPtr;

    virtual void OnStartLeading(TEpoch epoch) = 0;
    virtual void OnStopLeading() = 0;
    virtual void OnStartFollowing(TPeerId leaderId, TEpoch epoch) = 0;
    virtual void OnStopFollowing() = 0;

    virtual TPeerPriority GetPriority() = 0;
    virtual Stroka FormatPriority(TPeerPriority priority) = 0;

    virtual ~IElectionCallbacks() { }
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

    DECLARE_THREAD_AFFINITY_SLOT(ServiceThread); // thread of Invoker

    /*!
     * \note Thread Affinity: Any thread
     */
    TElectionManager(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr invoker,
        IElectionCallbacks::TPtr electionCallbacks,
        NRpc::TServer::TPtr server);
    ~TElectionManager();

    /*!
     * \note Thread Affinity: Any thread
     */
    void Start();

    /*!
     * \note Thread Affinity: Any thread
     */
    void Stop();

    /*!
     * \note Thread Affinity: Any thread
     */
    void Restart();
    
private:
    typedef TElectionManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

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
    TCancelableInvoker::TPtr EpochInvoker;
    
    typedef yhash_set<TPeerId> TPeerSet;
    TPeerSet AliveFollowers;
    TPeerSet PotentialFollowers;

    TDelayedInvoker::TCookie PingTimeoutCookie;
    TIntrusivePtr<TFollowerPinger> FollowerPinger;

    TConfig Config;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr Invoker;
    IElectionCallbacks::TPtr ElectionCallbacks;

    RPC_SERVICE_METHOD_DECL(NElectionManager::NProto, PingFollower);
    RPC_SERVICE_METHOD_DECL(NElectionManager::NProto, GetStatus);

    void RegisterMethods();

    /*!
     * \note Thread Affinity: Any thread
     */
    void Reset();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void OnLeaderPingTimeout();
    
    /*!
     * \note Thread Affinity: ServiceThread
     */
    void DoStart();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void DoStop();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StartVotingRound();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StartVoteFor(TPeerId voteId, const TEpoch& voteEpoch);

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StartVoteForSelf();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StartLeading();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StartFollowing(TPeerId leaderId, const TEpoch& epoch);

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StopLeading();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StopFollowing();

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StartEpoch(TPeerId leaderId, const TEpoch& epoch);

    /*!
     * \note Thread Affinity: ServiceThread
     */
    void StopEpoch();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
