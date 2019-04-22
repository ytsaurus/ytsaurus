#pragma once

#include "public.h"

#include <yt/core/bus/tcp/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TTraceManagerConfig
    : public NYTree::TYsonSerializable
{
public:
    TDuration CleanupPeriod;
    i64 TracesBufferSize;

    TTraceManagerConfig()
    {
        RegisterParameter("cleanup_period", CleanupPeriod)
            .Default(TDuration::Seconds(1));

        RegisterParameter("traces_buffer_size", TracesBufferSize)
            .Default(16_MB);
    }
};

DEFINE_REFCOUNTED_TYPE(TTraceManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing

