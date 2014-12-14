#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NTransactionServer {

////////////////////////////////////////////////////////////////////////////////

class TTransactionManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration MaxTransactionTimeout;

    TTransactionManagerConfig()
    {
        RegisterParameter("max_transaction_timeout", MaxTransactionTimeout)
            .Default(TDuration::Minutes(60));
    }
};

DEFINE_REFCOUNTED_TYPE(TTransactionManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TTimestampManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration CalibrationPeriod;
    TDuration CommitAdvance;
    int MaxTimestampsPerRequest;
    TDuration RequestBackoffTime;

    TTimestampManagerConfig()
    {
        RegisterParameter("calibration_period", CalibrationPeriod)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("commit_advance", CommitAdvance)
            .GreaterThan(TDuration::MilliSeconds(1000))
            .Default(TDuration::MilliSeconds(30000));
        RegisterParameter("max_timestamps_per_request", MaxTimestampsPerRequest)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("request_backoff_time", RequestBackoffTime)
            .Default(TDuration::MilliSeconds(100));
    }
};

DEFINE_REFCOUNTED_TYPE(TTimestampManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTransactionServer
} // namespace NYT
