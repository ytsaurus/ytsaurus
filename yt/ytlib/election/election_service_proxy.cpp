#include "election_service_proxy.h"

#include <yt/core/misc/common.h>

namespace NYT {
namespace NElection {

////////////////////////////////////////////////////////////////////////////////

Stroka TElectionServiceProxy::GetServiceName()
{
    return "ElectionService";
}

TElectionServiceProxy::TElectionServiceProxy(NRpc::IChannelPtr channel)
    : TProxyBase(channel, GetServiceName())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NElection
} // namespace NYT

