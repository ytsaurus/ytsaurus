#pragma once

#include "public.h"

#include <core/rpc/public.h>

#include <core/misc/common.h>
#include <core/misc/error.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct IEvaluateCallbacks
{
    virtual ~IEvaluateCallbacks()
    { }

    virtual IReaderPtr GetReader(const TDataSplit& dataSplit) = 0;

};

struct ICoordinateCallbacks
    : public IEvaluateCallbacks
{
    virtual ~ICoordinateCallbacks()
    { }

    virtual TFuture<TErrorOr<std::vector<TDataSplit>>> SplitFurther(
        const TDataSplit& dataSplit) = 0;

    virtual IReaderPtr Delegate(
        const TQueryFragment& fragment,
        const TDataSplit& colocatedDataSplit) = 0;

};

class IExecutor
    : public TRefCounted
{
public:
    virtual TAsyncError Execute(
        const TQueryFragment& fragment,
        TWriterPtr writer) = 0;

};

////////////////////////////////////////////////////////////////////////////////

IReaderPtr DelegateToPeer(const TQueryFragment& subfragment, NRpc::IChannelPtr channel);

IExecutorPtr CreateEvaluator(IEvaluateCallbacks* callbacks);
IExecutorPtr CreateCoordinator(ICoordinateCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

