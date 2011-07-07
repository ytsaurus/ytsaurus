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
        TMasterStateManager::TPtr masterStateManager,
        TCellManager::TPtr cellManager,
        TMasterId leaderId,
        TMasterEpoch epoch,
        IInvoker::TPtr epochInvoker,
        IInvoker::TPtr serviceInvoker);

    void Terminate();

private:
    typedef TMasterStateManagerProxy TProxy;

    void SchedulePing();
    void SendPing();
    void OnSendPing(TProxy::TRspPingLeader::TPtr response);

    TConfig Config;
    TMasterStateManager::TPtr MasterStateManager;
    TCellManager::TPtr CellManager;
    TMasterId LeaderId;
    TMasterEpoch Epoch;
    IInvoker::TPtr EpochInvoker;
    IInvoker::TPtr ServiceInvoker;
    bool Terminated;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
