#pragma once

#include <yt/core/rpc/client.h>

#include <yt/core/tracing/proto/trace_service.pb.h>

namespace NYT {
namespace NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTraceServiceProxy, TraceService);

    DEFINE_RPC_PROXY_METHOD(NProto, SendBatch);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTracing
} // namespace NYT

