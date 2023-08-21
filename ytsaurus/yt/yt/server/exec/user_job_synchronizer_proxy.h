#pragma once

#include <yt/yt/server/lib/user_job/proto/user_job_synchronizer_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NUserJob {

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TUserJobSynchronizerServiceProxy, SynchronizerService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NUserJob::NProto, ExecutorPrepared);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJob
