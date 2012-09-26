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
        ((NoSuchSnapshot)(21))
        ((NoSuchChangeLog)(22))
        ((InvalidEpoch)(23))
        ((InvalidVersion)(24))
        ((InvalidStatus)(25))
        ((SnapshotAlreadyInProgress)(26))
    );

    explicit TMetaStateManagerProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, ReadSnapshot);
    DEFINE_RPC_PROXY_METHOD(NProto, ReadChangeLog);
    DEFINE_RPC_PROXY_METHOD(NProto, GetSnapshotInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, GetChangeLogInfo);
    DEFINE_RPC_PROXY_METHOD(NProto, ApplyMutations);
    DEFINE_RPC_PROXY_METHOD(NProto, AdvanceSegment);
    DEFINE_RPC_PROXY_METHOD(NProto, PingFollower);
    DEFINE_RPC_PROXY_METHOD(NProto, LookupSnapshot);
    DEFINE_RPC_PROXY_METHOD(NProto, GetQuorum);
    DEFINE_RPC_PROXY_METHOD(NProto, BuildSnapshot);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
