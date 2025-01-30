#pragma once

#include "public.h"
#include "query_common.h"

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/actions/future.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using TExecuteQueryCallback = std::function<TFuture<void>(
    const TQueryPtr& query,
    TDataSource dataSource,
    IUnversionedRowsetWriterPtr writer)>;

////////////////////////////////////////////////////////////////////////////////

struct IExecutor
    : public virtual TRefCounted
{
    virtual TQueryStatistics Execute(
        const TPlanFragment& planFragment,
        const TConstExternalCGInfoPtr& externalCGInfo,
        const IUnversionedRowsetWriterPtr& writer,
        const TQueryOptions& options,
        const TFeatureFlags& requestFeatureFlags) = 0;
};

DEFINE_REFCOUNTED_TYPE(IExecutor)

////////////////////////////////////////////////////////////////////////////////

struct IPrepareCallbacks
{
    virtual ~IPrepareCallbacks() = default;

    //! Returns the initial split for a given path.
    virtual TFuture<TDataSplit> GetInitialSplit(const NYPath::TYPath& path) = 0;

    //! Fetches externally defined functions.
    virtual void FetchFunctions(TRange<TString> names, const TTypeInferrerMapPtr& typeInferrers) = 0;
};

////////////////////////////////////////////////////////////////////////////////

using TJoinSubqueryEvaluator = std::function<ISchemafulUnversionedReaderPtr(std::vector<TRow>, TRowBufferPtr)>;
using TJoinSubqueryProfiler = std::function<TJoinSubqueryEvaluator(int)>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

