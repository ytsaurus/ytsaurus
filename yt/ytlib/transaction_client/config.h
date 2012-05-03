#pragma once

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

struct TTransactionManagerConfig
    : public TConfigurable
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
