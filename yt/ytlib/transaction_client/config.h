#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <ytlib/hydra/config.h>

namespace NYT {
namespace NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public TYsonSerializable
{
public:
    //! An internal between successive transaction pings.
    TDuration PingPeriod;

    TTransactionManagerConfig()
    {
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(5));
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TRemoteTimestampProviderConfig
    : public NHydra::TPeerDiscoveryConfig
{ };

DEFINE_REFCOUNTED_TYPE(TRemoteTimestampProviderConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionClient
} // namespace NYT
