#pragma once

#include <yt/yt_proto/yt/client/job_proxy/proto/job_api_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TJobApiServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TJobApiServiceProxy, JobApiService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NJobProxy::NJobApi::NProto, OnProgressSaved);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
