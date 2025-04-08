#pragma once

#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TConsumeSubqueryStatistics = std::function<void(TQueryStatistics statistics)>;

using TGetMergeJoinDataSource = std::function<TDataSource(size_t keyPrefix)>;

using TExecuteForeign = std::function<TFuture<TQueryStatistics>(
    TPlanFragment fragment,
    IUnversionedRowsetWriterPtr writer)>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinRowsProducer)

struct IJoinRowsProducer
    : public TRefCounted
{
    virtual ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> joinKeys,
        TRowBufferPtr buffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinRowsProducer)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinSubqueryProfiler)

struct IJoinSubqueryProfiler
    : public TRefCounted
{
    virtual IJoinRowsProducerPtr Profile(int joinIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinSubqueryProfiler)

////////////////////////////////////////////////////////////////////////////////

IJoinSubqueryProfilerPtr CreateJoinProfiler(
    TConstQueryPtr query,
    const TQueryOptions& queryOptions,
    size_t minKeyWidth,
    bool orderedExecution,
    IMemoryChunkProviderPtr memoryChunkProvider,
    TConsumeSubqueryStatistics consumeSubqueryStatistics,
    TGetMergeJoinDataSource getMergeJoinDataSource,
    TExecuteForeign executeForeign,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
