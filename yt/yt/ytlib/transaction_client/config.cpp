#include "config.h"

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

TTransactionManagerConfig::TTransactionManagerConfig()
{
    RegisterParameter("rpc_timeout", RpcTimeout)
        .Default(TDuration::Seconds(30));
    RegisterParameter("default_ping_period", DefaultPingPeriod)
        .Default(TDuration::Seconds(5));
    RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
        .Default(TDuration::Seconds(30));

    RegisterPreprocessor([&] {
        RetryAttempts = 100;
        RetryTimeout = TDuration::Minutes(3);
    });

    RegisterPostprocessor([&] () {
        if (DefaultTransactionTimeout <= DefaultPingPeriod) {
            THROW_ERROR_EXCEPTION("\"default_transaction_timeout\" must be greater than \"default_ping_period\"");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
