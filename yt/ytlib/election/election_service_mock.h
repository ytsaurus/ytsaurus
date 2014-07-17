#pragma once

#include "election_service_proxy.h"

#include <core/rpc/service_detail.h>

#include <yt/unittests/framework.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionServiceMock
    : public NRpc::TServiceBase
{
public:
    explicit TElectionServiceMock(IInvokerPtr defaultInvoker)
        : TServiceBase(
            defaultInvoker,
            TElectionServiceProxy::GetServiceName(),
            NLog::TLogger())
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PingFollower));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(GetStatus));
    }

    MOCK_RPC_SERVICE_METHOD(NElection::NProto, PingFollower);
    MOCK_RPC_SERVICE_METHOD(NElection::NProto, GetStatus);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT
