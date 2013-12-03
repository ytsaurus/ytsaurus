#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/misc/common.h>
#include <core/misc/error.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class IExecutor
    : public TRefCounted
{
public:
    virtual TAsyncError Execute(
        const TPlanFragment& fragment,
        IWriterPtr writer) = 0;

};

IExecutorPtr CreateEvaluator(
    IInvokerPtr invoker,
    IEvaluateCallbacks* callbacks);

IExecutorPtr CreateCoordinatedEvaluator(
    IInvokerPtr invoker,
    ICoordinateCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

