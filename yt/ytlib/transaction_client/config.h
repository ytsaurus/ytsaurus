#pragma once

#include "public.h"

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig
    : public TYsonSerializable
{
    //! An internal between successive transaction pings.
    TDuration PingPeriod;

    TTransactionManagerConfig()
    {
        Register("ping_period", PingPeriod)
            .Default(TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NTransactionClient
} // namespace NYT
