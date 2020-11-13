#pragma once

#include "public.h"
#include "callbacks.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public TRefCounted
{
public:
    explicit TEvaluator(
        TExecutorConfigPtr config,
        const NProfiling::TRegistry& profiler = {},
        IMemoryUsageTrackerPtr memoryTracker = nullptr);

    ~TEvaluator();

    TQueryStatistics Run(
        TConstBaseQueryPtr fragment,
        ISchemafulUnversionedReaderPtr reader,
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

