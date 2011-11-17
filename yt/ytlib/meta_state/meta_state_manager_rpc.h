#pragma once

#include "common.h"
#include "meta_state_manager_rpc.pb.h"

#include "../rpc/service.h"
#include "../rpc/client.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManagerProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TMetaStateManagerProxy> TPtr;

    RPC_DECLARE_PROXY(MetaStateManager,
        ((InvalidSegmentId)(1))
        ((InvalidEpoch)(2))
        ((InvalidVersion)(3))
        ((InvalidStatus)(4))
        ((IOError)(5))
        ((Busy)(6))
    );

    TMetaStateManagerProxy(NRpc::IChannel::TPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    RPC_PROXY_METHOD(NMetaState::NProto, ReadSnapshot);
    RPC_PROXY_METHOD(NMetaState::NProto, ReadChangeLog);
    RPC_PROXY_METHOD(NMetaState::NProto, GetSnapshotInfo);
    RPC_PROXY_METHOD(NMetaState::NProto, GetChangeLogInfo);
    RPC_PROXY_METHOD(NMetaState::NProto, ApplyChanges);
    RPC_PROXY_METHOD(NMetaState::NProto, AdvanceSegment);
    RPC_PROXY_METHOD(NMetaState::NProto, PingFollower);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
