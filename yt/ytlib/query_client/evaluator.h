#pragma once

#include "public.h"
#include "callbacks.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IMemoryUsageTracker)

struct IMemoryUsageTracker
    : public TIntrinsicRefCounted
{
    virtual TError TryAcquire(size_t size) = 0;
    virtual void Release(size_t size) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryUsageTracker)

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public TIntrinsicRefCounted
{
public:
    explicit TEvaluator(
        TExecutorConfigPtr config,
        const NProfiling::TProfiler& profiler = NProfiling::TProfiler(),
        IMemoryUsageTrackerPtr memoryTracker = nullptr);

    TQueryStatistics Run(
        TConstBaseQueryPtr fragment,
        ISchemafulReaderPtr reader,
        IUnversionedRowsetWriterPtr writer,
        TJoinSubqueryProfiler joinProfiler,
        TConstFunctionProfilerMapPtr functionProfilers,
        TConstAggregateProfilerMapPtr aggregateProfilers,
        const TQueryBaseOptions& options);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEvaluator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

