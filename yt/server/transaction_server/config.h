#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public TYsonSerializable
{
public:
    TDuration DefaultTransactionTimeout;
    TDuration MaxTransactionTimeout;

    TTransactionManagerConfig()
    {
        RegisterParameter("default_transaction_timeout", DefaultTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Seconds(15));
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .GreaterThan(TDuration())
            .Default(TDuration::Minutes(60));
    }
};

class TTimestampManagerConfig
    : public TYsonSerializable
{
public:
    // TODO(babenko)

    TTimestampManagerConfig()
    {
        // TODO(babenko)
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
