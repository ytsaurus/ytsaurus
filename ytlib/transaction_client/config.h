#pragma once

#include "public.h"

#include <yt/client/transaction_client/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NRpc::TRetryingChannelConfig
{
public:
    //! Timeout for all RPC requests to participants.
    TDuration RpcTimeout;

    //! Default internal between consecutive transaction pings.
    TDuration DefaultPingPeriod;

    //! Default transaction timeout to be used if not given explicitly on
    //! transaction start.
    TDuration DefaultTransactionTimeout;

    TTransactionManagerConfig()
    {
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(30));
        RegisterParameter("default_ping_period", DefaultPingPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
            .Default(TDuration::Seconds(15));

        RegisterPostprocessor([&] () {
            if (DefaultTransactionTimeout <= DefaultPingPeriod) {
                THROW_ERROR_EXCEPTION("\"default_transaction_timeout\" must be greater than \"default_ping_period\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
