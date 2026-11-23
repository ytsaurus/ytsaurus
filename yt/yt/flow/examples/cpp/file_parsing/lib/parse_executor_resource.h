#pragma once

#include "parse_executor.h"

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NFlow::NFileParsingExample {

////////////////////////////////////////////////////////////////////////////////

struct TParseExecutorResourceParameters
    : public TParseExecutorConfig
{
    //! The stub parse of a file of size |size| takes |size * ParseTimePerSizeUnit|.
    TDuration ParseTimePerSizeUnit;

    REGISTER_YSON_STRUCT(TParseExecutorResourceParameters);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TParseExecutorResourceParameters);

////////////////////////////////////////////////////////////////////////////////

//! The #TParseExecutor of a worker, shared by all partitions of the worker.
/*!
 *  The parses run on the resource's own thread pool with a thread per slot, outside of the jobs.
 *  The parse itself is a stub: it takes time proportional to the file size and returns a row
 *  per size unit.
 */
class TParseExecutorResource
    : public TResourceBase
{
public:
    YT_FLOW_EXTEND_PARAMETERS(TParseExecutorResourceParameters);

    using TResourceBase::TResourceBase;
    ~TParseExecutorResource() override;

    TFuture<void> Load(const THashMap<TResourceId, IResourcePtr>& dependencies) override;

    //! Submits |task| to the executor; the start callback of an async processor.
    TFuture<TParseResultPtr> Parse(TParseTaskPtr task);

private:
    // Both are set by #Load(), which completes before any job gets the resource.
    NConcurrency::IThreadPoolPtr ThreadPool_;
    TParseExecutorPtr Executor_;
};

DEFINE_REFCOUNTED_TYPE(TParseExecutorResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NFileParsingExample
