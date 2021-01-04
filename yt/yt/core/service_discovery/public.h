#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NServiceDiscovery {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((EndpointSetDoesNotExist) (20000))
    ((EndpointResolveFailed)   (20001))
    ((UnknownResolveStatus)    (20002))
);

DECLARE_REFCOUNTED_STRUCT(IServiceDiscovery)

struct TEndpoint;
struct TEndpointSet;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery
