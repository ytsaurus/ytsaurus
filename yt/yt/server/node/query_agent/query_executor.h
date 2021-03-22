#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/query_client/public.h>
#include <yt/yt/ytlib/query_client/query_common.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::TQueryStatistics ExecuteSubquery(
    TQueryAgentConfigPtr config,
    NQueryClient::TFunctionImplCachePtr functionImplCache,
    NClusterNode::TBootstrap* const bootstrap,
    NQueryClient::IEvaluatorPtr evaluator,
    NQueryClient::TConstQueryPtr query,
    NQueryClient::TConstExternalCGInfoPtr externalCGInfo,
    std::vector<NQueryClient::TDataSource> dataSources,
    NQueryClient::IUnversionedRowsetWriterPtr writer,
    IMemoryChunkProviderPtr memoryChunkProvider,
    IInvokerPtr invoker,
    const NChunkClient::TClientChunkReadOptions& chunkReadOptions,
    const NQueryClient::TQueryOptions& queryOptions,
    TServiceProfilerGuard& profilerGuard);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
