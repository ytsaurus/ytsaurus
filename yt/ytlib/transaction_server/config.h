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

    TTransactionManagerConfig()
    {
        Register("default_transaction_timeout", DefaultTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(15));
        Register("transaction_abort_backoff_time", TransactionAbortBackoffTime)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(15));
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NTransactionServer
} // namespace NYT
