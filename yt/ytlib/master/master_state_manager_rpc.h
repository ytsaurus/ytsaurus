#pragma once

#include "common.h"
#include "master_state_manager.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TMetaStateManagerProxy> TPtr;

    DECLARE_DERIVED_ENUM(NRpc::EErrorCode, EErrorCode,
        ((InvalidSegmentId)(1))
        ((InvalidEpoch)(2))
        ((InvalidVersion)(3))
        ((InvalidState)(4))
        ((IOError)(5))
    );

    static Stroka GetServiceName() 
    {
        return "MetaStateManager";
    }

    TMetaStateManagerProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NRpcMetaStateManager, ScheduleSync);
    RPC_PROXY_METHOD(NRpcMetaStateManager, Sync);
    RPC_PROXY_METHOD(NRpcMetaStateManager, ReadSnapshot);
    RPC_PROXY_METHOD(NRpcMetaStateManager, ReadChangeLog);
    RPC_PROXY_METHOD(NRpcMetaStateManager, GetSnapshotInfo);
    RPC_PROXY_METHOD(NRpcMetaStateManager, GetChangeLogInfo);
    RPC_PROXY_METHOD(NRpcMetaStateManager, ApplyChanges);
    RPC_PROXY_METHOD(NRpcMetaStateManager, CreateSnapshot);
    RPC_PROXY_METHOD(NRpcMetaStateManager, PingLeader);
};

////////////////////////////////////////////////////////////////////////////////

}
