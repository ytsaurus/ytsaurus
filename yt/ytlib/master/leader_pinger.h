#pragma once

#include "common.h"

#include "../master/master_state_manager.h"

namespace NYT {

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

} // namespace
