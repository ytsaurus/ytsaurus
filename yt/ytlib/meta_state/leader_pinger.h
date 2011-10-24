#pragma once

#include "common.h"
#include "meta_state_manager.h"
#include "meta_state_manager_rpc.h"
#include "cell_manager.h"

#include "../actions/invoker_util.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TLeaderPinger
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TLeaderPinger> TPtr;

    struct TConfig
    {
        TDuration PingInterval;
        TDuration RpcTimeout;

        TConfig()
            : PingInterval(TDuration::MilliSeconds(1000))
            , RpcTimeout(TDuration::MilliSeconds(1000))
        { }
    };

    TLeaderPinger(
        const TConfig& config,
        TMetaStateManager::TPtr metaStateManager,
        TCellManager::TPtr cellManager,
        TPeerId leaderId,
        TEpoch epoch,
        IInvoker::TPtr serviceInvoker);

    void Stop();

private:
    typedef TMetaStateManagerProxy TProxy;

    void SchedulePing();
    void SendPing();
    void OnSendPing(TProxy::TRspPingLeader::TPtr response);

    TConfig Config;
    TMetaStateManager::TPtr MetaStateManager;
    TCellManager::TPtr CellManager;
    TPeerId LeaderId;
    TEpoch Epoch;
    TCancelableInvoker::TPtr CancelableInvoker;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
