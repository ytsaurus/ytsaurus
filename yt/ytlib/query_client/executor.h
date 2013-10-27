#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/misc/common.h>
#include <core/misc/error.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello@): Replace me.
typedef void* IMegaWriterPtr;

struct IEvaluateCallbacks
{
    virtual ~IEvaluateCallbacks()
    { }

    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) = 0;
    virtual IMegaWriterPtr GetWriter() = 0;
};

struct ICoordinateCallbacks
    : public IEvaluateCallbacks
{
    virtual ~ICoordinateCallbacks()
    { }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(const TDataSplit& dataSplit) = 0;
    virtual IExecutorPtr GetColocatedExecutor(const TDataSplit& dataSplit) = 0;
};

class IExecutor
    : public TRefCounted
{
public:
    virtual IReaderPtr Execute(const TQueryFragment& fragment) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IExecutorPtr CreateRemoteExecutor(NRpc::IChannelPtr channel);
IExecutorPtr CreateEvaluator(IEvaluateCallbacks* callbacks);
IExecutorPtr CreateCoordinator(ICoordinateCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

