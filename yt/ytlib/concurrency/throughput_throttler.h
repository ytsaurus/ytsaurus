#pragma once

#include "public.h"

#include <ytlib/actions/future.h>

#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/ypath/public.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////
   
class TThroughputThrottlerConfig
    : public TYsonSerializable
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

//! Returns a throttler from #config.
IThroughputThrottlerPtr CreateLimitedThrottler(TThroughputThrottlerConfigPtr config);

//! Creates a wrapper that delegates all calls to #underlyingThrottler and
//! captures profiling statistics.
IThroughputThrottlerPtr CreateProfilingThrottlerWrapper(
    IThroughputThrottlerPtr underlyingThrottler,
    const NYPath::TYPath& pathPrefix);

//! Returns a throttler that imposes no throughput limit.
IThroughputThrottlerPtr GetUnlimitedThrottler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

