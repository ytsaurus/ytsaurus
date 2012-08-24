#pragma once

#include "common.h"
#include "public.h"
#include "config.h"
#include "election_manager_proxy.h"
#include "cell_manager.h"

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/actions/invoker.h>
#include <ytlib/rpc/service.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

struct IElectionCallbacks
    : public virtual TRefCounted
{
    virtual void OnStartLeading() = 0;
    virtual void OnStopLeading() = 0;
    virtual void OnStartFollowing() = 0;
    virtual void OnStopFollowing() = 0;

    virtual TPeerPriority GetPriority() = 0;
    virtual Stroka FormatPriority(TPeerPriority priority) = 0;
};

typedef TIntrusivePtr<IElectionCallbacks> IElectionCallbacksPtr;

////////////////////////////////////////////////////////////////////////////////

struct TEpochContext
    : public TIntrinsicRefCounted
{
    TEpochContext()
        : LeaderId(InvalidPeerId)
        , CancelableContext(New<TCancelableContext>())
    { }

    TPeerId LeaderId;
    TEpochId EpochId;
    TInstant StartTime;
    TCancelableContextPtr CancelableContext;
};

typedef TIntrusivePtr<TEpochContext> TEpochContextPtr;

////////////////////////////////////////////////////////////////////////////////

class TElectionManager
    : public NRpc::TServiceBase
{
public:
    TElectionManager(
        TElectionManagerConfigPtr config,
        TCellManagerPtr cellManager,
        IInvokerPtr controlInvoker,
        IElectionCallbacksPtr electionCallbacks);

    ~TElectionManager();

    /*!
     * \note Thread affinity: any.
     */
    void Start();

    /*!
     * \note Thread affinity: any.
     */
    void Stop();

    /*!
     * \note Thread affinity: any.
     */
    void Restart();

    //! Gets info for monitoring in YSON format.
    /*!
     * \note Thread affinity: any.
     */
    void GetMonitoringInfo(NYTree::IYsonConsumer* consumer);

    //! Returns the current current epoch context.
    /*!
     *  \note Thread affinity: any.
     */
    TEpochContextPtr GetEpochContext();
    
private:
    typedef TElectionManager TThis;
    typedef TElectionManagerProxy TProxy;
    typedef TProxy::EErrorCode EErrorCode;

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

typedef TIntrusivePtr<TElectionManager> TElectionManagerPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
