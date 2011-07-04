#pragma once

#include "common.h"
#include "master_state_manager.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMasterStateManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TMasterStateManagerProxy> TPtr;

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode,
        ((InvalidSegmentId)(1))
        ((InvalidEpoch)(2))
        ((InvalidStateId)(3))
        ((InvalidState)(4))
        ((IOError)(5))
    );

    static Stroka GetServiceName() 
    {
        return "MasterStateManager";
    }

    TMasterStateManagerProxy(NRpc::TChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    {}

    RPC_PROXY_METHOD(NRpcMasterStateManager, ScheduleSync);
    RPC_PROXY_METHOD(NRpcMasterStateManager, Sync);
    RPC_PROXY_METHOD(NRpcMasterStateManager, ReadSnapshot);
    RPC_PROXY_METHOD(NRpcMasterStateManager, ReadChangeLog);
    RPC_PROXY_METHOD(NRpcMasterStateManager, GetSnapshotInfo);
    RPC_PROXY_METHOD(NRpcMasterStateManager, GetChangeLogInfo);
    RPC_PROXY_METHOD(NRpcMasterStateManager, ApplyChange);
    RPC_PROXY_METHOD(NRpcMasterStateManager, CreateSnapshot);
    RPC_PROXY_METHOD(NRpcMasterStateManager, PingLeader);
};

////////////////////////////////////////////////////////////////////////////////

}
