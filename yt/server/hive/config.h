#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

class THiveManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between consequent Ping requests to remote Hive Manager.
    TDuration PingPeriod;

    //! Timeout for all RPC requests exchanged by Hive Managers.
    TDuration RpcTimeout;

    THiveManagerConfig()
    {
        RegisterParameter("ping_period", PingPeriod)
            .Default(TDuration::Seconds(15));
        RegisterParameter("rpc_timeout", RpcTimeout)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(THiveManagerConfig)

class TCellDirectorySynchronizerConfig
    : public NYTree::TYsonSerializable
{
public:
    //! Interval between consequent SyncCells requests to the primary Hive Manager.
    TDuration SyncPeriod;

    TCellDirectorySynchronizerConfig()
    {
        RegisterParameter("sync_period", SyncPeriod)
            .Default(TDuration::Seconds(15));
    }
};

DEFINE_REFCOUNTED_TYPE(TCellDirectorySynchronizerConfig)

class TTransactionSupervisorConfig
    : public NYTree::TYsonSerializable
{
public:
    TTransactionSupervisorConfig()
    { }
};

DEFINE_REFCOUNTED_TYPE(TTransactionSupervisorConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
