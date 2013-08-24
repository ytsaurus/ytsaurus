#include "stdafx.h"
#include "election_service_proxy.h"

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

Stroka TElectionServiceProxy::GetServiceName()
{
    return "ElectionManager";
}

TElectionServiceProxy::TElectionServiceProxy(NRpc::IChannelPtr channel)
    : TProxyBase(channel, GetServiceName())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

