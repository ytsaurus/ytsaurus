#pragma once

#include "public.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

IClientRequestControlPtr RedirectServiceRequest(
    IServiceContextPtr context,
    IChannelPtr channel);

IServicePtr CreateRedirectorService(
    const TServiceId& serviceId,
    IChannelPtr sinkChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
