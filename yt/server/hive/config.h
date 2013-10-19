#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class THiveManagerConfig
    : public TYsonSerializable
{
public:
    //! Interval period consequent Ping requests to remote Hive instances.
    TDuration PingPeriod;

    //! Timeout for all RPC requests exchanged by cells.
    TDuration RpcTimeout;

    THiveManagerConfig()
    {
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(15));
    }
};

class TTransactionSupervisorConfig
    : public TYsonSerializable
{
public:

    TTransactionSupervisorConfig()
    {
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
