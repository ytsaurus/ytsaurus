#pragma once

#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TConsumeSubqueryStatistics = std::function<void(TQueryStatistics statistics)>;

using TGetPrefetchJoinDataSource = std::function<std::optional<TDataSource>()>;

using TExecutePlan = std::function<TFuture<TQueryStatistics>(
    TPlanFragment fragment,
    IUnversionedRowsetWriterPtr writer)>;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinRowsProducer)

struct IJoinRowsProducer
    : public virtual TRefCounted
{
    virtual ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> joinKeys,
        TRowBufferPtr buffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinRowsProducer)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJoinProfiler)

struct IJoinProfiler
    : public virtual TRefCounted
{
    virtual IJoinRowsProducerPtr Profile() = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinProfiler)

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr CreateJoinSubqueryProfiler(
    TConstJoinClausePtr joinClause,
    TExecutePlan executePlan,
    TConsumeSubqueryStatistics consumeSubqueryStatistics,
    TGetPrefetchJoinDataSource getPrefetchJoinDataSource,
    IMemoryChunkProviderPtr memoryChunkProvider,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
