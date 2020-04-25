#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/server/lib/misc/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/query_client/public.h>
#include <yt/ytlib/query_client/query_common.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct IQuerySubexecutor
    : public virtual TRefCounted
{
    virtual TFuture<NQueryClient::TQueryStatistics> Execute(
        NQueryClient::TConstQueryPtr query,
        NQueryClient::TConstExternalCGInfoPtr externalCGInfo,
        std::vector<NQueryClient::TDataRanges> dataSources,
        NTableClient::IUnversionedRowsetWriterPtr writer,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        const NQueryClient::TQueryOptions& options,
        TServiceProfilerGuard& profilerGuard) = 0;
};

DEFINE_REFCOUNTED_TYPE(IQuerySubexecutor)

////////////////////////////////////////////////////////////////////////////////

IQuerySubexecutorPtr CreateQuerySubexecutor(
    TQueryAgentConfigPtr config,
    NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent

