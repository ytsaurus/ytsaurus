#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TDynamicTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;
    int MaxTransactionDepth;

    TDynamicTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .Default(TDuration::Minutes(60));
        RegisterParameter("max_transaction_depth", MaxTransactionDepth)
            .GreaterThan(0)
            .Default(32);
    }
};

DEFINE_REFCOUNTED_TYPE(TDynamicTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionServer
