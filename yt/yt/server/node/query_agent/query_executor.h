#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/public.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/query/base/public.h>
#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

NQueryClient::TQueryStatistics ExecuteSubquery(
    TQueryAgentConfigPtr config,
    NQueryClient::TFunctionImplCachePtr functionImplCache,
    NTabletNode::IBootstrap* const bootstrap,
    NQueryClient::IEvaluatorPtr evaluator,
    NQueryClient::TConstQueryPtr query,
    NQueryClient::TConstExternalCGInfoPtr externalCGInfo,
    std::vector<NQueryClient::TDataSource> dataSources,
    NQueryClient::IUnversionedRowsetWriterPtr writer,
    IMemoryChunkProviderPtr memoryChunkProvider,
    IInvokerPtr invoker,
    NQueryClient::TQueryOptions queryOptions,
    TServiceProfilerGuard& profilerGuard);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
