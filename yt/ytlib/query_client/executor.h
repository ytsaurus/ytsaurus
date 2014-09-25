#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/misc/common.h>
#include <core/misc/error.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IExecutor
    : public virtual TRefCounted
{
    virtual TFuture<TErrorOr<TQueryStatistics>> Execute(
        const TPlanFragmentPtr& fragment,
        ISchemafulWriterPtr writer) = 0;

};

DEFINE_REFCOUNTED_TYPE(IExecutor)

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateEvaluator(
    TExecutorConfigPtr config,
    IInvokerPtr invoker,
    IEvaluateCallbacks* callbacks);

IExecutorPtr CreateCoordinator(
    TExecutorConfigPtr config,
    IInvokerPtr invoker,
    ICoordinateCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

