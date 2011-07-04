#pragma once

#include "common.h"
#include "leader_lookup.h"
#include "election_manager_rpc.h"
#include "cell_manager.h"

#include "../actions/invoker.h"
#include "../actions/delayed_invoker.h"
#include "../rpc/service.h"
#include "../rpc/client.h"
#include "../rpc/server.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef TGUID TMasterEpoch;
typedef i64 TMasterPriority;

////////////////////////////////////////////////////////////////////////////////

struct IElectionCallbacks
{
    virtual void StartLeading(TMasterEpoch epoch) = 0;
    virtual void StopLeading() = 0;
    virtual void StartFollowing(TMasterId leaderId, TMasterEpoch epoch) = 0;
    virtual void StopFollowing() = 0;
    virtual TMasterPriority GetPriority() = 0;
    virtual Stroka FormatPriority(TMasterPriority priority) = 0;

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

    TElectionManager(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr invoker,
        IElectionCallbacks* electionCallbacks,
        NRpc::TServer* server);
    ~TElectionManager();

    void Start();
    void Stop();
    void Restart();
    
    IInvoker::TPtr GetEpochInvoker() const;

private:
    typedef TElectionManagerProxy TProxy;
    // TODO: use typed service exception

    class TVotingRound;
    class TFollowerPinger;

    TProxy::EState State;
    
    // Voting parameters.
    TMasterId VoteId;
    TMasterEpoch VoteEpoch;

    // Epoch parameters.
    TMasterId LeaderId;
    TGUID Epoch;
    TInstant EpochStart;
    TCancelableInvoker::TPtr EpochInvoker;
    
    typedef yhash_set<TMasterId> TMasterSet;
    TMasterSet AliveFollowers;
    TMasterSet PotentialFollowers;

    TDelayedInvoker::TCookie PingTimeoutCookie;
    TIntrusivePtr<TFollowerPinger> FollowerPinger;

    TConfig Config;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr Invoker;
    IElectionCallbacks* ElectionCallbacks;

    RPC_SERVICE_METHOD_DECL(NRpcElectionManager, PingFollower);
    RPC_SERVICE_METHOD_DECL(NRpcElectionManager, GetStatus);

    void RegisterMethods();

    void Reset();

    void OnLeaderPingTimeout();
    
    void DoStart(); // Invoker thread
    void DoStop(); // Invoker thread

    void StartVotingRound(); // Invoker thread
    void StartVoteFor(TMasterId voteId, const TMasterEpoch& voteEpoch); // Invoker thread
    void StartVoteForSelf(); // Invoker thread
    void StartLeading(); // Invoker thread
    void StartFollowing(TMasterId leaderId, const TMasterEpoch& epoch); // Invoker thread
    void StopLeading(); // Invoker thread
    void StopFollowing(); // Invoker thread
    void StartEpoch(TMasterId leaderId, const TMasterEpoch& epoch); // Invoker thread
    void StopEpoch(); // Invoker thread
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
