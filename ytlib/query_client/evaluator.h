#pragma once

#include "public.h"
#include "callbacks.h"

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TEvaluator
    : public TIntrinsicRefCounted
{
public:
    explicit TEvaluator(TExecutorConfigPtr config);
    ~TEvaluator();

    TQueryStatistics RunWithExecutor(
        TConstQueryPtr fragment,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        TExecuteQueryCallback executeCallback,
        TConstFunctionProfilerMapPtr functionProfilers,
        TConstAggregateProfilerMapPtr aggregateProfilers,
        bool enableCodeCache);

    TQueryStatistics Run(
        TConstQueryPtr fragment,
        ISchemafulReaderPtr reader,
        ISchemafulWriterPtr writer,
        TConstFunctionProfilerMapPtr functionProfilers,
        TConstAggregateProfilerMapPtr aggregateProfilers,
        bool enableCodeCache);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TEvaluator)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

