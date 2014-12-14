#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/rpc/config.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Internal between consecutive transaction pings.
    TDuration PingPeriod;

    //! Default transaction timeout to be used if not given explicitly on
    //! transaction start.
    TDuration DefaultTransactionTimeout;

    TTransactionManagerConfig()
    {
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProviderConfig
    : public NRpc::TBalancingChannelConfig
{
public:
    //! Timeout for RPC requests to timestamp provider.
    TDuration RpcTimeout;

    //! Interval between consecutive current timestamp updates. 
    TDuration UpdatePeriod;

    TRemoteTimestampProviderConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(3));
        RegisterParameter("update_period", UpdatePeriod)
            .Default(TDuration::Seconds(3));
    }
};

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
