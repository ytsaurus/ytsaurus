#pragma once

#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/callbacks.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>


namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct TQueryExecutorRowBufferTag
{ };

struct TInferRangesResult
{
    std::vector<std::pair<TDataSource, TString>> DataSources;
    TConstQueryPtr ResultQuery;

    // COMPAT(lukyan)
    bool SortedDataSource;
};

std::pair<TDataSource, TConstQueryPtr> InferRanges(
    const IColumnEvaluatorCachePtr& columnEvaluatorCache,
    TConstQueryPtr query,
    const TDataSource& dataSource,
    const TQueryOptions& options,
    TRowBufferPtr rowBuffer,
    const NLogging::TLogger& Logger);

std::vector<std::pair<TDataSource, TString>> CoordinateDataSources(
    const NHiveClient::ICellDirectoryPtr& cellDirectory,
    const NNodeTrackerClient::TNetworkPreferenceList& networks,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TDataSource& dataSource,
    TRowBufferPtr rowBuffer);

IExecutorPtr CreateQueryExecutor(
    IMemoryChunkProviderPtr memoryChunkProvider,
    NApi::NNative::IConnectionPtr connection,
    IColumnEvaluatorCachePtr columnEvaluatorCache,
    IEvaluatorPtr evaluator,
    NNodeTrackerClient::INodeChannelFactoryPtr nodeChannelFactory,
    TFunctionImplCachePtr functionImplCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
