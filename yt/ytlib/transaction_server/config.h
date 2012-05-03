#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig
    : public TConfigurable
{
    TDuration DefaultTransactionTimeout;
    TDuration TransactionAbortBackoffTime;
    TDuration MaximumTransactionTimeout;

    TTransactionManagerConfig()
    {
        Register("default_transaction_timeout", DefaultTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(15));
        Register("transaction_abort_backoff_time", TransactionAbortBackoffTime)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(15));
        Register("maximum_transaction_timeout", DefaultTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(30));
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NTransactionServer
} // namespace NYT
