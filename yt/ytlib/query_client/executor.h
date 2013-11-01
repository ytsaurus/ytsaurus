#pragma once

#include "public.h"

#include <ytlib/node_tracker_client/public.h>

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
        const TQueryFragment& fragment,
        TWriterPtr writer) = 0;

};

IReaderPtr DelegateToPeer(
    const TQueryFragment& subfragment,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NRpc::IChannelPtr channel);

IExecutorPtr CreateEvaluator(IInvokerPtr invoker, IEvaluateCallbacks* callbacks);
IExecutorPtr CreateCoordinator(IInvokerPtr invoker, ICoordinateCallbacks* callbacks);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

