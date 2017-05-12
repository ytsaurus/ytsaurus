#pragma once

#include "public.h"

#include <yt/ytlib/api/client.h>

namespace NYT {
namespace NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyConnectionConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TRpcProxyConnectionConfig()
    {
        RegisterParameter("addresses", Addresses)
            .NonEmpty();
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(3));
    }

    std::vector<TString> Addresses;
    TDuration PingPeriod;
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyConnectionConfig)

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyClientConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TRpcProxyClientConfig()
    {
        // This constructor intentionally left blank.
    }
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpcProxy
} // namespace NYT
