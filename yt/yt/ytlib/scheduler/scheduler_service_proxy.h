#pragma once

#include "public.h"

#include <yt/yt/ytlib/scheduler/proto/scheduler_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TOperationServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TOperationServiceProxy, SchedulerService,
        .SetProtocolVersion(4));

    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, StartOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, AbortOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, SuspendOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, ResumeOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, CompleteOperation);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, UpdateOperationParameters);
    DEFINE_RPC_PROXY_METHOD(NScheduler::NProto, GetAllocationBriefInfo);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
