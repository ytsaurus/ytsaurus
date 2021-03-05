#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

namespace NYT::NServiceDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
{
    TString Id;
    TString Protocol;
    TString Fqdn;
    TString IP4Address;
    TString IP6Address;
    int Port;

    //! Identifies whether this endpoint is ready to serve traffic
    //! according to the provider.
    bool Ready;
};

struct TEndpointSet
{
    TString Id;

    std::vector<TEndpoint> Endpoints;
};

////////////////////////////////////////////////////////////////////////////////

struct IServiceDiscovery
    : public virtual TRefCounted
{
    virtual TFuture<TEndpointSet> ResolveEndpoints(
        const TString& cluster,
        const TString& endpointSetId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IServiceDiscovery)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NServiceDiscovery
