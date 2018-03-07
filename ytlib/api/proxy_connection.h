#pragma once

#include "connection.h"

namespace NYT {
namespace NApi {

////////////////////////////////////////////////////////////////////////////////

struct TProxyInfo
{
    TString Address;
};

struct TDiscoverProxyOptions
{ };

struct IProxyConnection
    : public IConnection
{
    virtual TFuture<std::vector<TProxyInfo>> DiscoverProxies(
        const TDiscoverProxyOptions& options = TDiscoverProxyOptions()) = 0;
};

DEFINE_REFCOUNTED_TYPE(IProxyConnection)

////////////////////////////////////////////////////////////////////////////////

} // namespace NApi
} // namespace NYT

