#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTimestampServer {

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

} // namespace NYT::NTimestampServer
