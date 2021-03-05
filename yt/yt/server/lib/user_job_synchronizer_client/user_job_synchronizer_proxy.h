#pragma once

#include <yt/yt/server/lib/user_job_synchronizer_client/proto/user_job_synchronizer_service.pb.h>

#include <yt/yt/core/rpc/client.h>

namespace NYT::NUserJobSynchronizerClient {

////////////////////////////////////////////////////////////////////////////////

class TUserJobSynchronizerServiceProxy
    : public NRpc::TProxyBase
{
public:
    DEFINE_RPC_PROXY(TUserJobSynchronizerServiceProxy, SynchronizerService,
        .SetProtocolVersion(0));

    DEFINE_RPC_PROXY_METHOD(NUserJobSynchronizerClient::NProto, ExecutorPrepared);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NUserJobSynchronizerClient
