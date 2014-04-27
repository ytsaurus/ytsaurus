#pragma once

#include <core/rpc/client.h>

#include <core/tracing/trace_service.pb.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "TraceService";
    }

    explicit TTraceServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NProto, SendBatch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

