#pragma once

#include "public.h"
#include "callbacks.h"

#include <yt/core/profiling/profiler.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IEvaluator
    : public virtual TRefCounted
{
    virtual TQueryStatistics Run(
        const TConstBaseQueryPtr& query,
        const ISchemafulUnversionedReaderPtr& reader,
        const IUnversionedRowsetWriterPtr& writer,
        const TJoinSubqueryProfiler& joinProfiler,
        const TConstFunctionProfilerMapPtr& functionProfilers,
        const TConstAggregateProfilerMapPtr& aggregateProfilers,
        const IMemoryChunkProviderPtr& memoryChunkProvider,
        const TQueryBaseOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IEvaluator)

IEvaluatorPtr CreateEvaluator(
    TExecutorConfigPtr config,
    const NProfiling::TRegistry& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

