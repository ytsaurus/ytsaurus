#pragma once

#include "common.h"
#include "public.h"
#include "config.h"
#include "leader_lookup.h"
#include "election_manager_proxy.h"
#include "cell_manager.h"

#include <ytlib/misc/delayed_invoker.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/actions/invoker.h>
#include <ytlib/rpc/service.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

struct IElectionCallbacks
    : public virtual TRefCounted
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

    TElectionManager(
        TElectionManagerConfig* config,
        TCellManager* cellManager,
        IInvoker::TPtr controlInvoker,
        IElectionCallbacks* electionCallbacks);

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
    
private:
    typedef TElectionManager TThis;
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
    TCancelableContextPtr ControlEpochContext;
    IInvoker::TPtr ControlEpochInvoker;
    
    typedef yhash_set<TPeerId> TPeerSet;
    TPeerSet AliveFollowers;
    TPeerSet PotentialFollowers;

    TDelayedInvoker::TCookie PingTimeoutCookie;
    TIntrusivePtr<TFollowerPinger> FollowerPinger;

    TElectionManagerConfigPtr Config;
    TCellManagerPtr CellManager;
    IInvoker::TPtr ControlInvoker;
    IElectionCallbacks::TPtr ElectionCallbacks;

    // Corresponds to #ControlInvoker.
    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    DECLARE_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);

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

    void UpdateState(TProxy::EState newState);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
