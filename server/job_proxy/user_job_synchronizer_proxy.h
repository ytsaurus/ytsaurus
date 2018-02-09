#pragma once

#include "public.h"

#include <yt/core/rpc/client.h>

#include <yt/server/job_proxy/user_job_synchronizer_service.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TUserJobSynchronizerServiceProxy, SynchronizerService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, SatellitePrepared);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, ExecutorPrepared);
    DEFINE_RPC_PROXY_METHOD(NJobProxy::NProto, UserJobFinished);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
