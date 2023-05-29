#pragma once

#include "public.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/net/address.h>

namespace NYT::NDns {

////////////////////////////////////////////////////////////////////////////////

struct TDnsResolveOptions
{
    bool EnableIPv4 = true;
    bool EnableIPv6 = true;
};

void FormatValue(TStringBuilderBase* builder, const TDnsResolveOptions options, TStringBuf spec);
TString ToString(const TDnsResolveOptions& options);

////////////////////////////////////////////////////////////////////////////////

struct IDnsResolver
{
    virtual ~IDnsResolver() = default;

    virtual TFuture<NNet::TNetworkAddress> Resolve(
        const TString& hostName,
        const TDnsResolveOptions& options) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDns

