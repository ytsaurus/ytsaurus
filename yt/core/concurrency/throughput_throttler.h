#pragma once

#include "public.h"

#include <core/actions/future.h>

#include <core/ytree/yson_serializable.h>

#include <core/ypath/public.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////
   
class TThroughputThrottlerConfig
    : public NYTree::TYsonSerializable
{
public:
    TThroughputThrottlerConfig()
    {
        RegisterParameter("period", Period)
            .Default(TDuration::MilliSeconds(1000));
        RegisterParameter("limit", Limit)
            .Default(Null)
            .GreaterThanOrEqual(0);
    }

    //! Period for which the bandwidth limit applies.
    TDuration Period;

    //! Limit on average throughput (per sec). Null means unlimited.
    TNullable<i64> Limit;
};

DEFINE_REFCOUNTED_TYPE(TThroughputThrottlerConfig)

////////////////////////////////////////////////////////////////////////////////

//! Enables async operation to throttle based on throughput limit.
/*!
 *  This interface and its implementations are vastly inspired by |DataTransferThrottler| class from Hadoop
 *  but return promise instead of using direct sleep calls.
 */
struct IThroughputThrottler
    : public virtual TRefCounted
{
    //! Assuming that we are about to transfer #count bytes,
    //! returns a future that is set when enough time has passed
    //! to ensure proper bandwidth utilization.
    /*!
     *  \note Thread affinity: any
     */
    virtual TFuture<void> Throttle(i64 count) = 0;
};

DEFINE_REFCOUNTED_TYPE(IThroughputThrottler)

//! Returns a throttler from #config.
IThroughputThrottlerPtr CreateLimitedThrottler(
    TThroughputThrottlerConfigPtr config,
    NLog::TLogger logger = NLog::TLogger(),
    NProfiling::TProfiler profiler = NProfiling::TProfiler());

//! Returns a throttler that imposes no throughput limit.
IThroughputThrottlerPtr GetUnlimitedThrottler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

