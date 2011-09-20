#pragma once

#include "common.h"
#include "meta_state_manager_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TMetaStateManagerProxy> TPtr;

    DECLARE_POLY_ENUM2(NRpc::EErrorCode, EErrorCode,
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

    RPC_PROXY_METHOD(NMetaState::NProto, ScheduleSync);
    RPC_PROXY_METHOD(NMetaState::NProto, Sync);
    RPC_PROXY_METHOD(NMetaState::NProto, ReadSnapshot);
    RPC_PROXY_METHOD(NMetaState::NProto, ReadChangeLog);
    RPC_PROXY_METHOD(NMetaState::NProto, GetSnapshotInfo);
    RPC_PROXY_METHOD(NMetaState::NProto, GetChangeLogInfo);
    RPC_PROXY_METHOD(NMetaState::NProto, ApplyChanges);
    RPC_PROXY_METHOD(NMetaState::NProto, AdvanceSegment);
    RPC_PROXY_METHOD(NMetaState::NProto, PingLeader);
};

////////////////////////////////////////////////////////////////////////////////

}
