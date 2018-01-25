#pragma once

#include "public.h"

#include <yt/ytlib/scheduler/proto/job_prober_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TJobProberServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobProberServiceProxy, JobProberService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NProto, DumpInputContext);
    DEFINE_RPC_PROXY_METHOD(NProto, GetJobNode);
    DEFINE_RPC_PROXY_METHOD(NProto, Strace);
    DEFINE_RPC_PROXY_METHOD(NProto, SignalJob);
    DEFINE_RPC_PROXY_METHOD(NProto, AbandonJob);
    DEFINE_RPC_PROXY_METHOD(NProto, PollJobShell);
    DEFINE_RPC_PROXY_METHOD(NProto, AbortJob);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
