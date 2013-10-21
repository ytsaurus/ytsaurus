#pragma once

#include "public.h"

#include <core/rpc/client.h>

#include <ytlib/hive/timestamp_service.pb.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class TTimestampServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "TimestampService";
    }

    explicit TTimestampServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, GetTimestamp);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT

