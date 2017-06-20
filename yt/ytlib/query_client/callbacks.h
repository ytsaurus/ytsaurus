#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/ytlib/ypath/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/actions/future.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TExecuteQueryCallback = std::function<TFuture<TQueryStatistics>(
    const TQueryPtr& query,
    TDataRanges dataRanges,
    ISchemafulWriterPtr writer)>;

////////////////////////////////////////////////////////////////////////////////

struct IExecutor
    : public virtual TRefCounted
{
    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        TDataRanges dataSource,
        ISchemafulWriterPtr writer,
        const TQueryOptions& options) = 0;

};

DEFINE_REFCOUNTED_TYPE(IExecutor)

struct ISubexecutor
    : public virtual TRefCounted
{
    virtual TFuture<TQueryStatistics> Execute(
        TConstQueryPtr query,
        TConstExternalCGInfoPtr externalCGInfo,
        std::vector<TDataRanges> dataSources,
        ISchemafulWriterPtr writer,
        const TQueryOptions& options) = 0;

};

DEFINE_REFCOUNTED_TYPE(ISubexecutor)

////////////////////////////////////////////////////////////////////////////////

struct IPrepareCallbacks
{
    virtual ~IPrepareCallbacks() = default;

    //! Returns the initial splits for given paths.
    virtual TFuture<std::vector<TDataSplit>> GetInitialSplits(
        const std::vector<NYPath::TRichYPath>& paths,
        TTimestamp timestamp) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

