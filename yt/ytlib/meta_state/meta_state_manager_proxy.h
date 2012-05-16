#pragma once

#include "public.h"
#include <ytlib/meta_state/meta_state_manager.pb.h>

#include <ytlib/rpc/client.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TMetaStateManagerProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "MetaState";
    }

    DECLARE_ENUM(EErrorCode,
        ((NoSuchSnapshot)(1))
        ((NoSuchChangeLog)(2))
        ((InvalidEpoch)(3))
        ((InvalidVersion)(4))
        ((InvalidStatus)(5))
        ((SnapshotAlreadyInProgress)(6))
    );

    TMetaStateManagerProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, ReadSnapshot);
    DEFINE_RPC_PROXY_METHOD(NProto, ReadChangeLog);
    DEFINE_RPC_PROXY_METHOD(NProto, GetSnapshotInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChangeLogInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, ApplyChanges);
    DEFINE_RPC_PROXY_METHOD(NProto, AdvanceSegment);
    DEFINE_RPC_PROXY_METHOD(NProto, PingFollower);
    DEFINE_RPC_PROXY_METHOD(NProto, LookupSnapshot);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
