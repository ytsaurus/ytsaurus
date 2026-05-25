#pragma once

#include "public.h"

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

struct IJoinRowsProducer
    : public virtual TRefCounted
{
    virtual ISchemafulUnversionedReaderPtr FetchJoinedRows(
        std::vector<TRow> joinKeys,
        TRowBufferPtr buffer) = 0;
};

DEFINE_REFCOUNTED_TYPE(IJoinRowsProducer)

////////////////////////////////////////////////////////////////////////////////

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
    bool useOrderByInJoinSubqueries,
    bool allowHeavyRangeInferenceInJoins,
    std::optional<i64> cacheSize,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

IJoinProfilerPtr CreateJoinRowsetProfiler(
    TSharedRange<TRow> rowset,
    int foreignKeyPrefix,
    int joinKeySize,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

class TJoinProfilerRegistry
{
public:
    IJoinProfilerPtr GetJoinProfilerOrThrow(size_t index) const;
    void InsertJoinProfilerOrThrow(size_t index, IJoinProfilerPtr profiler);

private:
    THashMap<size_t, IJoinProfilerPtr> Profilers_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
